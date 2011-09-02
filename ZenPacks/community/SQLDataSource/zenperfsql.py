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

$Id: zenperfsql.py,v 2.4 2011/09/02 17:47:43 egor Exp $"""

__version__ = "$Revision: 2.4 $"[11:-2]

import logging
from copy import copy

import Globals
import zope.component
import zope.interface
from DateTime import DateTime
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
from SQLClient import SQLClient

# We retrieve our configuration data remotely via a Twisted PerspectiveBroker
# connection. To do so, we need to import the class that will be used by the
# configuration service to send the data over, i.e. DeviceProxy.
from Products.ZenUtils.Utils import unused
from Products.ZenCollector.services.config import DeviceProxy
unused(DeviceProxy)

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
                md5.new(subconfig[0]).hexdigest())

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

        # the configurationService attribute is the fully qualified class-name
        # of our configuration service that runs within ZenHub
        self.configurationService = 'ZenPacks.community.SQLDataSource.services.SqlPerfConfig'

    def buildOptions(self, parser):
        parser.add_option('--debug', dest='debug', default=False,
                               action='store_true',
                               help='Increase logging verbosity.')

    def postStartup(self):
        # turn on debug logging if requested
        logseverity = self.options.logseverity

class ZenPerfSqlTask(ObservableMixin):
    zope.interface.implements(IScheduledTask)

    #counter to keep track of total queries sent
    QUERIES = 0

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

    def _finished(self, result):
        """
        Callback activated when the task is complete so that final statistics
        on the collection can be displayed.
        """

        self._cleanup()
        if not isinstance(result, Failure):
            log.debug("Device %s [%s] scanned successfully",
                      self._devId, self._manageIp)
        else:
            log.debug("Device %s [%s] scanned failed, %s",
                      self._devId, self._manageIp, result.getErrorMessage())

        # give the result to the rest of the callback/errchain so that the
        # ZenCollector framework can keep track of the success/failure rate
        return result

    def _failure(self, result, comp=None):
        """
        Errback for an unsuccessful asynchronous connection or collection 
        request.
        """
        err = result.getErrorMessage()
        log.error("Device %s: %s", self._devId, err)
        collectorName = self._preferences.collectorName
        summary = "Could not fetch data from source"

        self._eventService.sendEvent(dict(
            summary=summary,
            message=summary + " (%s)"%err,
            component=comp or collectorName,
            eventClass='/Status/PyDBAPI',
            device=self._devId,
            severity=Error,
            agent=collectorName,
            ))

        # give the result to the rest of the errback chain
        return result


    def _sendEvents(self, devices):
        """
        Sent Error and Clear events 
        """
        message = "Could not fetch data"
        agent = self._preferences.collectorName,
        events = []
        for devId, components in devices.iteritems():
            errors = []
            for comp, severity in components.iteritems():
                if isinstance(severity, Failure):
                    message = severity.getErrorMessage()
                    severity = Error
                event = dict(
                    summary = "Could not fetch data",
                    message = message,
                    eventClass = '/Status/PyDBAPI',
                    device = devId,
                    component = comp,
                    severity = severity,
                    agent = agent,
                    )
                if severity == Error:
                    errors.append(event)
                else:
                    events.append(event)
            if len(errors) == len(components) > 0:
                event = errors[0]
                del event['component']
                events.append(event)
            else:
                events.extend(errors)
        for event in events:
            self._eventService.sendEvent(event)


    def _collectSuccessful(self, results):
        """
        Callback for a successful fetch of services from the remote device.
        """
        self.state = ZenPerfSqlTask.STATE_SQLC_PROCESS

        log.debug("Successful collection from %s [%s], results=%s",
                  self._devId, self._manageIp, results)

#        for q in self._queries.values():
#            ZenPerfSqlTask.QUERIES += len(q)

        if not results: return None
#        compstatus = {self._monitor:{'':Clear}}
        compstatus = {}
        for cs,tn,dpname,comp,expr,rrdPath,rrdType,rrdC,mm in self._datapoints:
            values = []
            if '/' not in tn: devId = self._monitor
            else: devId = tn.split('/')[0] or self._monitor
            if devId not in compstatus: compstatus[devId] = {}
            if comp not in compstatus[devId]: compstatus[devId][comp] = Clear
            for d in results.get(tn, []):
                if isinstance(d, Failure):
                    compstatus[devId][comp] = d
                    break
                if len(d) == 0: continue
                dpvalue = d.get(dpname, None)
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
            try:
                self._dataService.writeRRD( rrdPath,
                                            float(value),
                                            rrdType,
                                            rrdC,
                                            min=mm[0],
                                            max=mm[1])
            except Exception, e:
                compstatus[devId][comp] = Failure(e)
        self._sendEvents(compstatus)
        compstatus.clear()
        return results


    def _collectData(self):
        """
        Callback called after a connect or previous collection so that another
        collection can take place.
        """
        log.debug("Polling for SQL data from %s [%s]", 
                  self._devId, self._manageIp)

        self.state = ZenPerfSqlTask.STATE_SQLC_QUERY
        self._sqlc = SQLClient(self._taskConfig)
        cs = self._datapoints[0][0]
        d = self._sqlc.sortedQuery({cs: self._taskConfig.queries[cs]})
        d.addCallbacks(self._collectSuccessful, self._failure)
        return d


    def cleanup(self):
        self._cleanup()


    def _cleanup(self):
        if hasattr(self._sqlc, 'close'):
            self._sqlc.close()
        self._sqlc = None


    def doTask(self):
        log.debug("Scanning device %s [%s]", self._devId, self._manageIp)

        # try collecting events after a successful connect, or if we're
        # already connected

        d = self._collectData()

        # Add the _finished callback to be called in both success and error
        # scenarios. While we don't need final error processing in this task,
        # it is good practice to catch any final errors for diagnostic purposes.
        d.addCallback(self._finished)

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
