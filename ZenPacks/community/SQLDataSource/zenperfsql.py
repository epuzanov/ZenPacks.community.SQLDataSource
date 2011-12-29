################################################################################
#
# This program is part of the SQLDataSource Zenpack for Zenoss.
# Copyright (C) 2010, 2011 Egor Puzanov.
#
# This program can be used under the GNU General Public License version 2
# You can find full information here: http://www.zenoss.com/oss
#
################################################################################

__doc__="""zenperfsql

PB daemon-izable base class for creating sql collectors

$Id: zenperfsql.py,v 2.15 2011/12/29 20:52:23 egor Exp $"""

__version__ = "$Revision: 2.15 $"[11:-2]

import logging
import pysamba.twisted.reactor

import Globals
import zope.component
import zope.interface
from DateTime import DateTime

from copy import copy
import md5

from twisted.internet import defer, reactor
from twisted.python.failure import Failure

from Products.ZenCollector.daemon import CollectorDaemon
from Products.ZenCollector.interfaces import ICollectorPreferences,\
                                             IDataService,\
                                             IEventService,\
                                             IScheduledTask,\
                                             IScheduledTaskFactory
from Products.ZenCollector.tasks import SimpleTaskFactory,\
                                        SimpleTaskSplitter,\
                                        TaskStates
from Products.ZenEvents.ZenEventClasses import Error, Clear
from Products.ZenUtils.observable import ObservableMixin
from SQLClient import SQLClient, SQLPlugin

# We retrieve our configuration data remotely via a Twisted PerspectiveBroker
# connection. To do so, we need to import the class that will be used by the
# configuration service to send the data over, i.e. DeviceProxy.
from Products.ZenUtils.Utils import unused
from Products.ZenCollector.services.config import DeviceProxy
unused(DeviceProxy)
unused(Globals)

#
# creating a logging context for this module to use
#
log = logging.getLogger("zen.zenperfsql")

#
# RPN reverse calculation
#
import operator

OPERATORS = {
    '-': operator.add,
    '+': operator.sub,
    '/': operator.mul,
    '*': operator.truediv,
}

def rrpn(expression, value):
    oper = None
    try:
        stack = [float(value)]
        tokens = expression.split(',')
        tokens.reverse()
        for token in tokens:
            if token == 'now': token = DateTime()._t
            try:
                stack.append(float(token))
            except ValueError:
                if oper:
                    stack.append(OPERATORS[oper](stack.pop(-2), stack.pop()))
                oper = token
        val = OPERATORS[oper](stack.pop(-2), stack.pop())
        return val//1
    except:
        return value


class SubConfigurationTaskSplitter(SimpleTaskSplitter):
    """
    A drop-in replacement for original SubConfigurationTaskSplitter class.
    A task splitter that creates a single scheduled task by
    device, cycletime and other criteria.
    """

    subconfigName = 'datasources'

    def makeConfigKey(self, config, subconfig):
        raise NotImplementedError("Required method not implemented")

    def _newTask(self, name, configId, interval, config):
        """
        Handle the dirty work of creating a task
        """
        self._taskFactory.reset()
        self._taskFactory.name = name
        self._taskFactory.configId = configId
        self._taskFactory.interval = interval
        self._taskFactory.config = config
        return self._taskFactory.build()

    def _splitSubConfiguration(self, config):
        subconfigs = {}
        for subconfig in getattr(config, self.subconfigName):
            key = self.makeConfigKey(config, subconfig)
            subconfigList = subconfigs.setdefault(key, [])
            subconfigList.append(subconfig)
        return subconfigs

    def splitConfiguration(self, configs):
        # This name required by ITaskSplitter interface
        tasks = {}
        for config in configs:
            log.debug("Splitting config %s", config)

            # Group all of the subtasks under the same configId
            # so that updates clean up any previous tasks
            # (including renames)
            configId = config.id

            subconfigs = self._splitSubConfiguration(config)
            for key, subconfigGroup in subconfigs.items():
                name = ' '.join(map(str, key))
                interval = key[1]

                configCopy = copy(config)
                setattr(configCopy, self.subconfigName, subconfigGroup)

                tasks[name] = self._newTask(name,
                                            configId,
                                            interval,
                                            configCopy)
        return tasks

class ZenPerfSqlTaskSplitter(SubConfigurationTaskSplitter):
    """
    A task splitter that creates a single scheduled task by
    device, cycletime and other criteria.
    """
    subconfigName = 'datapoints'

    def makeConfigKey(self, config, subconfig):
        return (config.id, config.configCycleInterval,
                md5.new(' '.join(subconfig[0])).hexdigest())
#                ' '.join(subconfig[0]))


# Create an implementation of the ICollectorPreferences interface so that the
# ZenCollector framework can configure itself from our preferences.
class ZenPerfSqlPreferences(object):
    zope.interface.implements(ICollectorPreferences)

    def __init__(self):
        """
        Construct a new ZenPerfSqlPreferences instance and provide default
        values for needed attributes.
        """
        self.collectorName = "zenperfsql"
        self.defaultRRDCreateCommand = None
        self.cycleInterval = 5 * 60 # seconds
        self.configCycleInterval = 20 # minutes
        self.options = None
        self.sync = False
        self.maxTasks = 1

        # the configurationService attribute is the fully qualified class-name
        # of our configuration service that runs within ZenHub
        self.configurationService = 'ZenPacks.community.SQLDataSource.services.SqlPerfConfig'

    def buildOptions(self, parser):
        parser.add_option('--debug', dest='debug', default=False,
                               action='store_true',
                               help='Increase logging verbosity.')
        parser.add_option('--sync', dest='sync', default=False,
                               action="store_true",
                               help="Force Synchronous queries execution.")


    def postStartup(self):
        # turn on debug logging if requested
        logseverity = self.options.logseverity


class ZenPerfSqlTask(ObservableMixin):
    zope.interface.implements(IScheduledTask)

    STATE_SQLC_CONNECT = 'SQLC_CONNECT'
    STATE_SQLC_QUERY = 'SQLC_QUERY'
    STATE_SQLC_PROCESS = 'SQLC_PROCESS'

    def __init__(self,
                 taskName,
                 deviceId,
                 scheduleIntervalSeconds,
                 taskConfig):
        """
        Construct a new task instance to get SQL data.

        @param deviceId: the Zenoss deviceId to watch
        @type deviceId: string
        @param taskName: the unique identifier for this task
        @type taskName: string
        @param scheduleIntervalSeconds: the interval at which this task will be
               collected
        @type scheduleIntervalSeconds: int
        @param taskConfig: the configuration for this task
        """
        super(ZenPerfSqlTask, self).__init__()

        self.name = taskName
        self.configId = deviceId
        self.interval = scheduleIntervalSeconds
        self.state = TaskStates.STATE_IDLE

        self._taskConfig = taskConfig
        self._devId = deviceId
        self._manageIp = self._taskConfig.manageIp
        self._datapoints = self._taskConfig.datapoints
        self._thresholds = self._taskConfig.thresholds

        self._dataService = zope.component.queryUtility(IDataService)
        self._eventService = zope.component.queryUtility(IEventService)
        self._preferences = zope.component.queryUtility(ICollectorPreferences,
                                                        "zenperfsql")
        self._sqlc = None

    def _failure(self, result, comp=None):
        """
        Errback for an unsuccessful asynchronous connection or collection 
        request.
        """
        err = result.getErrorMessage()
        log.error("Device %s: %s", self._devId, err)
        collectorName = self._preferences.collectorName
        summary = "Could not fetch data"

        self._eventService.sendEvent(dict(
            summary=summary,
            message=summary + " (%s)"%err,
            component=comp or collectorName,
            eventClass='/Status/PyDBAPI',
            device=self._devId,
            severity=Error,
            agent=collectorName,
            ))

        self._cleanup()
        # give the result to the rest of the errback chain
        return result

    def _sendEvents(self, components):
        """
        Sent Error and Clear events 
        """
        events = []
        errors = []
        for (comp, metaType), severity in components.iteritems():
            event = dict(
                summary = "Could not fetch data",
                message = "Could not fetch data",
                eventClass = '/Status/%s' % metaType,
                device = self._devId,
                severity = severity,
                agent = self._preferences.collectorName,
                )
            if comp: event['component'] = comp
            else: event['eventClass'] = '/Status/PyDBAPI'
            if isinstance(severity, Failure):
                event['message'] = severity.getErrorMessage()
                event['severity'] = Error
                errors.append(event)
            else:
                events.append(event)
        if len(errors) == len(components) > 0:
            event = errors[0]
            del event['component']
            event['eventClass'] = '/Status/PyDBAPI'
            events.append(event)
        else:
            events.extend(errors)
        for event in events:
            if event['eventClass'] == '/Status/': continue
            self._eventService.sendEvent(event)

    def _collectSuccessful(self, results={}):
        """
        Callback for a successful fetch of services from the remote device.
        """
        self.state = ZenPerfSqlTask.STATE_SQLC_PROCESS

        log.debug("Successful collection from %s [%s], results=%s",
                  self._devId, self._manageIp, results)
        if not results: return None

        compstatus = {}
        for cs,tn,dpname,alias,comp,expr,rrdP,rrdT,rrdC,mm in self._datapoints:
            values = []
            compstatus[comp] = Clear
            tresults = results.get(tn, [])
            if isinstance(tresults, Failure):
                compstatus[comp] = tresults
                tresults = []
            for d in tresults:
                if len(d) == 0: continue
                dpvalue = d.get(alias, None)
                if dpvalue == None or dpvalue == '': continue
                elif type(dpvalue) is list: dpvalue = dpvalue[0]
                elif isinstance(dpvalue, DateTime): dpvalue = dpvalue._t
                if expr:
                    if expr.__contains__(':'):
                        for vmap in expr.split(','):
                            var, val = vmap.split(':')
                            if var.strip('"') != dpvalue: continue
                            dpvalue = int(val)
                            break
                    else:
                        dpvalue = rrpn(expr, dpvalue)
                values.append(dpvalue)
            if dpname.endswith('_count'): value = len(values)
            elif not values: continue
            elif len(values) == 1: value = values[0]
            elif dpname.endswith('_avg'):value = sum(values) / len(values)
            elif dpname.endswith('_sum'): value = sum(values)
            elif dpname.endswith('_max'): value = max(values)
            elif dpname.endswith('_min'): value = min(values)
            elif dpname.endswith('_first'): value = values[0]
            elif dpname.endswith('_last'): value = values[-1]
            else: value = sum(values) / len(values)
            try: self._dataService.writeRRD(rrdP,
                                            float(value),
                                            rrdT,
                                            rrdC,
                                            min=mm[0],
                                            max=mm[1])
            except Exception, e:
                compstatus[comp] = Failure(e)
        self._sendEvents(compstatus)
        compstatus.clear()
        self._cleanup()
        return results

    def cleanup(self):
        self._cleanup()

    def _cleanup(self):
        if self._sqlc: self._sqlc.close()
        self._sqlc = None

    def doTask(self):
        log.debug("Polling for SQL data from %s [%s]", 
                                                    self._devId, self._manageIp)
        self.state = ZenPerfSqlTask.STATE_SQLC_QUERY
        queries = self._taskConfig.queries[self._datapoints[0][0]].copy()
        self._sqlc = SQLClient(self._taskConfig)
        d = defer.maybeDeferred(self._sqlc.query,queries,self._preferences.sync)
        d.addCallback(self._collectSuccessful)
        d.addErrback(self._failure)
        # returning a Deferred will keep the framework from assuming the task
        # is done until the Deferred actually completes
        return d


#
# Collector Daemon Main entry point
#
if __name__ == '__main__':
    myPreferences = ZenPerfSqlPreferences()
    myTaskFactory = SimpleTaskFactory(ZenPerfSqlTask)
    myTaskSplitter = ZenPerfSqlTaskSplitter(myTaskFactory)
    daemon = CollectorDaemon(myPreferences, myTaskSplitter)
    daemon.run()
