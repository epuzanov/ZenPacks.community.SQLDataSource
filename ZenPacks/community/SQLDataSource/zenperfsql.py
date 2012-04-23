################################################################################
#
# This program is part of the SQLDataSource Zenpack for Zenoss.
# Copyright (C) 2010-2012 Egor Puzanov.
#
# This program can be used under the GNU General Public License version 2
# You can find full information here: http://www.zenoss.com/oss
#
################################################################################

__doc__="""zenperfsql

Run SQL Queries periodically and stores it results in RRD files.

$Id: zenperfsql.py,v 3.3 2012/04/23 16:15:45 egor Exp $"""

__version__ = "$Revision: 3.3 $"[11:-2]

import time
from datetime import datetime, timedelta
import logging
log = logging.getLogger("zen.zenperfsql")
from copy import copy

from twisted.internet import reactor, defer, error
from twisted.python.failure import Failure

import Globals
import zope.interface

from Products.ZenModel.ZVersion import VERSION as ZVERSION
from Products.ZenUtils.Utils import unused
from Products.ZenUtils.observable import ObservableMixin
from Products.ZenEvents.ZenEventClasses import Clear, Error
from Products.ZenRRD.CommandParser import ParsedResults

from Products.ZenCollector.daemon import CollectorDaemon
from Products.ZenCollector.interfaces import ICollectorPreferences,\
                                             IDataService,\
                                             IEventService,\
                                             IScheduledTask,\
                                             IScheduledTaskFactory
from Products.ZenCollector.tasks import SimpleTaskFactory,\
                                        SimpleTaskSplitter,\
                                        TaskStates
from ZenPacks.community.SQLDataSource.SQLClient import  adbapiExecutor, \
                                                        DataSourceConfig,\
                                                        DataPointConfig
try:
    from Products.ZenCollector.pools import getPool
except:
    from ZenPacks.community.SQLDataSource.SQLClient import getPool
from Products.ZenEvents import Event

from Products.DataCollector import Plugins
unused(Plugins)

# We retrieve our configuration data remotely via a Twisted PerspectiveBroker
# connection. To do so, we need to import the class that will be used by the
# configuration service to send the data over, i.e. DeviceProxy.
from Products.ZenCollector.services.config import DeviceProxy
unused(DeviceProxy)

COLLECTOR_NAME = "zenperfsql"
POOL_NAME = 'SqlConfigs'

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
            if token == 'now':
                token = time.time()
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


class SqlPerformanceCollectionPreferences(object):
    zope.interface.implements(ICollectorPreferences)

    def __init__(self):
        """
        Constructs a new SqlPerformanceCollectionPreferences instance and 
        provides default values for needed attributes.
        """
        self.collectorName = COLLECTOR_NAME
        self.defaultRRDCreateCommand = None
        self.configCycleInterval = 20 # minutes
        self.cycleInterval = 5 * 60 # seconds

        # The configurationService attribute is the fully qualified class-name
        # of our configuration service that runs within ZenHub
        self.configurationService = 'ZenPacks.community.SQLDataSource.services.SqlPerformanceConfig'

        # Provide a reasonable default for the max number of tasks
        self.maxTasks = 1

        # Will be filled in based on buildOptions
        self.options = None

    def buildOptions(self, parser):
        parser.add_option('--showconnectionstring',
                          dest='showconnectionstring',
                          action="store_true",
                          default=False,
                          help="Display the entire connection string, " \
                               " including any passwords.")

    def postStartup(self):
        pass


STATUS_EVENT = {'eventClass' : '/Status/PyDBAPI',
                'component' : 'zenperfsql',
}


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


class SqlPerCycletimeTaskSplitter(SubConfigurationTaskSplitter):
    subconfigName = 'datasources'

    def makeConfigKey(self, config, subconfig):
        return (config.id,subconfig.cycleTime,hash(subconfig.connectionString))


class SqlPerformanceCollectionTask(ObservableMixin):
    """
    A task that performs periodic performance collection.
    """
    zope.interface.implements(IScheduledTask)

    STATE_CONNECTING = 'CONNECTING'
    STATE_FETCH_DATA = 'FETCH_DATA'
    STATE_PARSE_DATA = 'PARSING_DATA'
    STATE_STORE_PERF = 'STORE_PERF_DATA'

    def __init__(self,
                 taskName,
                 configId,
                 scheduleIntervalSeconds,
                 taskConfig):
        """
        @param taskName: the unique identifier for this task
        @type taskName: string
        @param configId: configuration to watch
        @type configId: string
        @param scheduleIntervalSeconds: the interval at which this task will be
               collected
        @type scheduleIntervalSeconds: int
        @param taskConfig: the configuration for this task
        """
        super(SqlPerformanceCollectionTask, self).__init__()

        # Needed for interface
        self.name = taskName
        self.configId = configId
        self.state = TaskStates.STATE_IDLE
        self.interval = scheduleIntervalSeconds

        # The taskConfig corresponds to a DeviceProxy
        self._device = taskConfig

        self._devId = self._device.id
        self._manageIp = self._device.manageIp

        self._dataService = zope.component.queryUtility(IDataService)
        self._eventService = zope.component.queryUtility(IEventService)

        self._preferences = zope.component.queryUtility(ICollectorPreferences,
                                                        COLLECTOR_NAME)
        self._lastErrorMsg = ''

        self._executor = None

        self._datasources = taskConfig.datasources
        self._dbapiName = taskConfig.datasources[0].connectionString.split(',',
                                                        1)[0].strip('\'"')
        self.pool = getPool('adbapi executors')
        self.executed = 0

    def __str__(self):
        return "SQL schedule Name: %s configId: %s Datasources: %d" % (
               self.name, self.configId, len(self._datasources))

    def cleanup(self):
        self._cleanUpPool()
        self._close()

    def _getPoolKey(self):
        """
        Get the key under which the client should be stored in the pool.
        """
        return self._dbapiName

    def _cleanUpPool(self):
        """
        Close the connection currently associated with this task.
        """
        poolkey = self._getPoolKey()
        if poolkey in self.pool and self.pool[poolkey]._running < 1:
            self.pool[poolkey] = None
            del self.pool[poolkey]

    def doTask(self):
        """
        Contact to one device and return a deferred which gathers data from
        the device.

        @return: Deferred actions to run against a device configuration
        @rtype: Twisted deferred object
        """
        # See if we need to connect first before doing any collection
        d = defer.maybeDeferred(self._connect)
        d.addCallbacks(self._connectCallback, self._failure)
        d.addCallback(self._fetchPerf)

        # Call _finished for both success and error scenarios
        d.addBoth(self._finished)

        # Wait until the Deferred actually completes
        return d

    def _connect(self):
        """
        check if executor for specific dbapi in pool if not create a executor.
        """

        poolKey = self._getPoolKey()
        executor = self.pool.get(poolKey, None)
        if executor is None:
            self.state = SqlPerformanceCollectionTask.STATE_CONNECTING
            log.debug("Creating %s executor object", poolKey)
            executor = adbapiExecutor()
            self.pool[poolKey] = executor
        self._executor = executor
        return executor

    def _close(self):
        """
        If a local datasource executor, do nothing.

        If an SSH datasource executor, relinquish a connection to the remote device.
        """
        if self._executor and self._executor._running < 1:
            self._executor =  None
            poolkey = self._getPoolKey()
            if poolkey in self.pool:
                self.pool[poolkey] = None
                del self.pool[poolkey]

    def _failure(self, reason):
        """
        Twisted errBack to log the exception for a single device.

        @parameter reason: explanation of the failure
        @type reason: Twisted error instance
        """
        msg = reason.getErrorMessage()
        if not msg: # Sometimes we get blank error messages
            msg = reason.__class__
        msg = '%s %s' % (self._devId, msg)

        if self._lastErrorMsg != msg:
            self._lastErrorMsg = msg
            if msg:
                log.error(msg)

        if reason:
            self._eventService.sendEvent(STATUS_EVENT,
                                     device=self._devId,
                                     summary=msg,
                                     severity=Event.Error)
        return reason

    def _connectCallback(self, result):
        """
        Callback called after a successful connect to the remote device.
        """
        log.debug("Connected to %s [%s]", self._devId, self._manageIp)
        return result

    def _fetchPerf(self, ignored):
        """
        Get performance data for all the monitored components on a device

        @parameter ignored: required to keep Twisted's callback chain happy
        @type ignored: result of previous callback
        """
        self.state = SqlPerformanceCollectionTask.STATE_FETCH_DATA

        # Bundle up the list of tasks
        deferredCmds = []
        for datasource in self._datasources:
            log.debug("Datasource %s %squery:'%s'", datasource.name,
                self._preferences.options.showconnectionstring and \
                "connection string: '%s', " % datasource.connectionString or "",
                datasource.sqlp)
            datasource.deviceConfig = self._device
            task = self._executor.submit(datasource)
            task.addBoth(self._processDatasourceResults)
            deferredCmds.append(task)

        # Run the tasks
        dl = defer.DeferredList(deferredCmds, consumeErrors=True)
        dl.addCallback(self._parseResults)
        dl.addCallback(self._storeResults)
        dl.addCallback(self._updateStatus)
        return dl

    def _processDatasourceResults(self, datasource):
        """
        Process a single datasource's result

        @parameter datasource: results rows
        @type datasource: DataSourceConfig object
        """
        result = ParsedResults()
        if isinstance(datasource.result, Failure):
            msg = datasource.result.getErrorMessage()
            datasource.result.cleanFailure()
            datasource.result = None
            ev = self._makeQueryEvent(datasource, msg)
            if ev['severity'] not in ('Clear', 'Info', 'Debug'):
                ev['stderr'] = msg
            result.events.append(ev)
            return datasource, result
        msg = 'Datasource %s query completed successfully' % (datasource.name)
        ev = self._makeQueryEvent(datasource, msg, Clear)
        result.events.append(ev)
        for dp in datasource.points:
            values = []
            for row in datasource.result:
                dpvalue = row.get(dp.alias, None)
                if dpvalue == None or dpvalue == '':
                    continue
                elif type(dpvalue) is list:
                    dpvalue = dpvalue[0]
                elif isinstance(dpvalue, datetime):
                    dpvalue = time.mktime(dpvalue.timetuple())
                elif isinstance(dpvalue, timedelta):
                    dpvalue = dpvalue.seconds
                if dp.expr:
                    if dp.expr.__contains__(':'):
                        for vmap in dp.expr.split(','):
                            var, val = vmap.split(':')
                            if var.strip('"') != dpvalue: continue
                            dpvalue = int(val)
                            break
                    else:
                        dpvalue = rrpn(dp.expr, dpvalue)
                values.append(dpvalue)
            if dp.id.endswith('_count'): value = len(values)
            elif not values: value = None
            elif len(values) == 1: value = values[0]
            elif dp.id.endswith('_avg'):value = sum(values) / len(values)
            elif dp.id.endswith('_sum'): value = sum(values)
            elif dp.id.endswith('_max'): value = max(values)
            elif dp.id.endswith('_min'): value = min(values)
            elif dp.id.endswith('_first'): value = values[0]
            elif dp.id.endswith('_last'): value = values[-1]
            else: value = sum(values) / len(values)
            result.values.append((dp, value))
        datasource.result = None
        return datasource, result


    def _parseResults(self, resultList):
        """
        Interpret the results retrieved from the commands and pass on
        the datapoint values and events.

        @parameter resultList: results of running the commands in a DeferredList
        @type resultList: array of (boolean, (datasource, result))
        """
        self.state = SqlPerformanceCollectionTask.STATE_PARSE_DATA
        parseableResults = []
        for success, results in resultList:
            parseableResults.append(results)
        return parseableResults

    def _storeResults(self, resultList):
        """
        Store the values in RRD files

        @parameter resultList: results of running the commands
        @type resultList: array of (datasource, dictionary)
        """
        self.state = SqlPerformanceCollectionTask.STATE_STORE_PERF
        for datasource, results in resultList:
            for dp, value in results.values:
                args = [dp.rrdPath,
                        value,
                        dp.rrdType,
                        dp.rrdCreateCommand,
                        datasource.cycleTime,
                        dp.rrdMin,
                        dp.rrdMax]
                if int(ZVERSION[0]) > 2:
                    threshData = {
                        'eventKey': datasource.getEventKey(dp),
                        'component': dp.component,
                        }
                    args.append(threshData)
                self._dataService.writeRRD(*args)

        return resultList

    def _updateStatus(self, resultList):
        """
        Send any accumulated events

        @parameter resultList: results of running the commands
        @type resultList: array of (datasource, dictionary)
        """
        for datasource, results in resultList:
            for ev in results.events:
                self._eventService.sendEvent(ev, device=self._devId)
        return resultList

    def _makeQueryEvent(self, datasource, msg, severity=None):
        """
        Create an event using the info in the DataSourceConfig object.
        """
        severity =  severity is None and datasource.severity or severity
        ev = dict(
                  device=self._devId,
                  component=datasource.component,
                  eventClass=datasource.eventClass,
                  eventKey=datasource.eventKey,
                  severity=severity,
                  summary=msg
        )
        return ev

    def _finished(self, result):
        """
        Callback activated when the task is complete

        @parameter result: results of the task
        @type result: deferred object
        """
        try:
            self._close()
        except Exception, ex:
            log.warn("Failed to close device %s: error %s" %
                     (self._devId, str(ex)))

        # Return the result so the framework can track success/failure
        return result

    def displayStatistics(self):
        """
        Called by the collector framework scheduler, and allows us to
        see how each task is doing.
        """
        display = "Active SQL Pools: %s\n"%len(self.pool)
        for n, p in self.pool.iteritems():
            display += '%s\n'%n
            if 0 < log.getEffectiveLevel() < 11:
                if p._cs:
                    display += '\tconnection string: %s\n'%p._cs
                elif p._taskQueue:
                    display += '\tconnection string: %s\n'%p._taskQueue[0]._cs
            display += '\tpool running: %s\n'%getattr(p._connection,'running',False)
            display += '\ttasks running: %s\n'%p._running
            display += '\ttasks queue: %s\n'%len(p._taskQueue)
            for t in p._taskQueue:
                display += '\t\t%s\n'%t._sql
        return display


if __name__ == '__main__':
    # Required for passing classes from zenhub to here
    from ZenPacks.community.SQLDataSource.SQLClient import DataSourceConfig,\
                                                            DataPointConfig

    myPreferences = SqlPerformanceCollectionPreferences()
    myTaskFactory = SimpleTaskFactory(SqlPerformanceCollectionTask)
    myTaskSplitter = SqlPerCycletimeTaskSplitter(myTaskFactory)
    daemon = CollectorDaemon(myPreferences, myTaskSplitter)
    daemon.run()

