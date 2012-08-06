################################################################################
#
# This program is part of the SQLDataSource Zenpack for Zenoss.
# Copyright (C) 2009-2012 Egor Puzanov.
#
# This program can be used under the GNU General Public License version 2
# You can find full information here: http://www.zenoss.com/oss
#
################################################################################

__doc__="""SQLClient

Gets performance data over python DB-API.

$Id: SQLClient.py,v 3.9 2012/08/06 20:16:22 egor Exp $"""

__version__ = "$Revision: 3.9 $"[11:-2]

if __name__ == "__main__":
    import pysamba.twisted.reactor
import logging
log = logging.getLogger("zen.SQLClient")

import Globals

from Products.DataCollector.BaseClient import BaseClient
from twisted.internet import defer, reactor
from twisted.python.failure import Failure
from twisted.enterprise import adbapi
from twisted.spread import pb

from threading import Timer
import sys
import re

try:
    from Products.ZenCollector.pools import getPool
except:
    ADBAPIPOOLS = {}
    def getPool(name, factory=None):
        return ADBAPIPOOLS

class TimeoutError(Exception):
    """
    Error for a defered call taking too long to complete
    """

    def __init__(self, *args):
        Exception.__init__(self)
        self.args = args

def parseConnectionString(cs='', options={}):
    try: args, kwargs = eval('(lambda *args,**kwargs:(args,kwargs))(%s)'%cs)
    except:
        args = []
        kwargs = {}
        for arg in cs.split(','):
            try:
                if arg.strip().startswith("'"):
                    arg = arg.strip("' ")
                    raise
                var, val = arg.strip().split('=', 1)
                if val.startswith('\'') or val.startswith('"'):
                    kwargs[var.strip()] = val.strip('\'" ')
                elif val.lower() == 'true':
                    kwargs[var.strip()] = True
                elif val.lower() == 'false':
                    kwargs[var.strip()] = False
                else:
                    kwargs[var.strip()] = float(val.strip())
            except: args.append(arg)
    kwargs.update(options)
    return args, kwargs

class adbapiClient(object):

    def __init__(self, cs):
        """
        @type cs: string
        @param cs: connection string
        """
        self.cs = cs
        self._dbapi = None
        self._connection = None

    def connect(self):
        try:
            args, kwargs = parseConnectionString(self.cs)
            self._connection = adbapi.ConnectionPool(*args, **kwargs)
            self._dbapi = self._connection.dbapi
            return defer.succeed(self)
        except Exception, ex:
            return defer.fail(ex)

    def close(self):
        if self._connection:
            self._connection.close()
            self._connection = None

    def _timeout(self, txn):
        if hasattr(txn._connection, 'cancel'):
            txn._connection.cancel()
        txn._cursor.close()

    def _convert(self, val, type):
        if val is None:
            if type == self._dbapi.STRING:
                return ''
            if type == self._dbapi.NUMBER:
                return 0
            return None
        if val and type == self._dbapi.NUMBER:
            return float(val)
        if type == self._dbapi.STRING:
            return str(val).strip()
        return val

    def runQuery(self, txn, sql, columns, timeout):
        """
        execute a sql query.

        @param txn: database cursor
        @type txn: dbapi.cursor or adbapi.Transaction
        @param sql: sql operation
        @type sql: string
        @param columns: columns to return
        @type columns: list
        @param timeout: timeout in seconds
        @type timeout: int
        """
        res = []
        t = Timer(timeout, self._timeout, txn)
        t.start()
        try:
            for q in re.split('[ \n]go[ \n]|;[ \n]', sql, re.I):
                if not q.strip(): continue
                txn.execute(q.strip())
        except Exception, ex:
            if t.isAlive():
                t.cancel()
            else:
                ex = TimeoutError('Timeout')
            raise ex
        t.cancel()
        if not txn.description:
            return res
        header, ct = zip(*[(h[0].lower(), h[1]) for h in txn.description])
        if set(columns).intersection(set(header)):
            varVal = False
        else:
            res.append({})
            varVal = True
        rows = txn.fetchmany()
        while rows:
            for row in rows:
                if varVal:
                    res[0][str(row[0]).lower()] = self._convert(row[-1], ct[-1])
                else:
                    res.append(dict(zip(header,
                                    [self._convert(*v) for v in zip(row,ct)])))
            rows = txn.fetchmany()
        return res

    def query(self, task):
        """
        execute a sql query.

        @param task: task to run
        @type task: DataSourceConfig
        """
        return self._connection.runInteraction(self.runQuery, task.sqlp,
                                                task.columns, task.timeout)


class dbapiClient(adbapiClient):

    def connect(self):
        fl = []
        args, kwargs = parseConnectionString(self.cs)
        if '.' in args[0]:
            fl.append(args[0].split('.')[-1])
        dbapi = __import__(args[0], globals(), locals(), fl)
        self._dbapi = dbapi
        self._connection = dbapi.connect(*args[1:], **kwargs)
        return self

    def _timeout(self, txn):
        if hasattr(self._connection, 'cancel'):
            self._connection.cancel()
        txn.close()

    def query(self, task):
        """
        execute a sql query.

        @param task: task to run
        @type task: DataSourceConfig
        """
        try:
            cursor = self._connection.cursor()
            result = self.runQuery(cursor,task.sqlp,task.columns,task.timeout)
            cursor.close()
        except Exception, ex:
            result = Failure(ex)
        return result

class adbapiExecutor(object):
    """
    Runs up to N queries at a time.  N is determined by the maxParrallel
    used to construct an instance, unlimited by default.
    """
    def __init__(self, maxParrallel=1):
        self._max = maxParrallel
        self._running = 0
        self._taskQueue = []
        self._connection = None
        self._cs = None
        self._currentTask = None

    def setMax(self, max):
        self._max = max
        reactor.callLater(0, self._runTask)

    def getMax(self):
        return self._max

    @property
    def running(self):
        return self._running

    @property
    def queued(self):
        return len(self._taskQueue)

    def submit(self, task):
        """
        submit a query to be executed. A deferred will be returned with the
        the result of the query.

        @param task: task to be executed
        @type task: DataSourceConfig
        """
        deferred = defer.Deferred()
        deferred.addBoth(self._taskFinished, task)
        task.result = deferred
        self._taskQueue.append(task)
        reactor.callLater(2, self._runTask)
        return deferred

    def _connect(self, task):
        if self._connection:
            if self._cs == task.connectionString:
                return defer.succeed(self._connection)
            else:
                self._connection.close()
                self._connection = None
        self._cs = task.connectionString
        if task.connectionString.startswith("'pywmidb'"):
            from pysambaClient import pysambaClient
            self._connection = pysambaClient(task.connectionString)
        else:
            self._connection = adbapiClient(task.connectionString)
        return defer.maybeDeferred(self._connection.connect)

    def _connected(self, connection, task):
        d = self._connection.query(task)
        d.addBoth(self._finished)
        return d

    def _notConnected(self, results):
        if self._connection:
            self._connection.close()
            self._connection = None
        self._finished(results)

    def _runTask(self):
        if not self._taskQueue or self._running >= self._max:
            return
        self._running += 1
        task = self._taskQueue[0]
        self._currentTask = hash((task.sqlp, str(task.columns)))
        d = self._connect(task)
        d.addCallback(self._connected, task)
        d.addErrback(self._notConnected)
        reactor.callLater(0, self._runTask)

    def _finished(self, results):
        nextTask = 0
        if isinstance(results, Failure):
            results.cleanFailure()
        for i in reversed(range(len(self._taskQueue))):
            if self._cs != self._taskQueue[i].connectionString: continue
            if self._currentTask != hash((self._taskQueue[i].sqlp,
                                        str(self._taskQueue[i].columns))):
                nextTask = i
                continue
            if nextTask > 0: nextTask -= 1
            task = self._taskQueue.pop(i)
            if isinstance(results, Failure):
                task.result.errback(results.getErrorMessage())
                continue
            if task.keybindings:
                kc, kv = zip(*[map(lambda v: str(v).strip().lower(), k) \
                                    for k in task.keybindings.iteritems()])
                kv = ''.join(kv)
            else: kc, kv = (), ''
            result = []
            for row in results:
                if kv == ''.join([str(row.get(k) or '').strip() for k in kc]
                    ).lower(): result.append(row)
            task.result.callback(result)
        if self._taskQueue:
            if nextTask > 0:
                self._taskQueue.insert(0, self._taskQueue.pop(nextTask))

    def _taskFinished(self, result, task):
        if self._running > 0:
            self._running -= 1
        if not self._taskQueue and self._running < 1:
            if self._connection:
                self._connection.close()
                self._connection = None
            self._cs = None
            self._currentTask = None
        reactor.callLater(0, self._runTask)
        task.result = result
        return task

class DataPointConfig(pb.Copyable, pb.RemoteCopy):
    """
    Represents data point
    """
    id = ''
    component = ''
    alias = ''
    expr = ''
    rrdPath = ''
    rrdType = None
    rrdCreateCommand = ''
    rrdMin = None
    rrdMax = None

    def __init__(self, id='', alias=''):
        """
        Objects initialization

        @param id: data points id
        @type id: string
        @param alias: column name
        @type alias: string
        """
        self.id = id
        self.alias = alias

    def __repr__(self):
        return ':'.join((self.id, self.alias))

pb.setUnjellyableForClass(DataPointConfig, DataPointConfig)


class DataSourceConfig(pb.Copyable, pb.RemoteCopy):
    """
    Holds the config of every query to be run
    """
    device = ''
    sql = ''
    sqlp = ''
    connectionString = ''
    keybindings = None
    ds = ''
    cycleTime = None
    eventClass = None
    eventKey = None
    severity = 3
    lastStart = 0
    lastStop = 0
    timeout = 180
    result = None

    def __init__(self, sqlp='', kbs={}, cs='', columns={}, sql=''):
        """
        Objects initialization

        @param sqlp: prepared sql string
        @type sqlp: string
        @param kbs: keybindings
        @type kbs: dictionary
        @param cs: database connection string
        @type cs: string
        @param sqlp: original sql string
        @type sqlp: string
        """
        self.sqlp = sqlp
        self.keybindings = kbs
        self.connectionString = cs
        self.points=[DataPointConfig(k,v.lower()) for k,v in columns.iteritems()]
        if not sql:
            self.sql = sqlp

    def __repr__(self):
        return self.sqlp

    @property
    def columns(self):
        return [dp.alias for dp in self.points]

    def getEventKey(self, point):
        # fetch datapoint name from filename path and add it to the event key
        return self.eventKey + '|' + point.rrdPath.split('/')[-1]

    def queryKey(self):
        "Provide a value that establishes the uniqueness of this query"
        return '%'.join(map(str,[self.cycleTime, self.severity,
                                self.connectionString, self.sql]))
    def __str__(self):
        return ' '.join(map(str, [
                        self.ds,
                        self.cycleTime,
                       ]))

pb.setUnjellyableForClass(DataSourceConfig, DataSourceConfig)


class SQLClient(BaseClient):
    """
    Implement the DataCollector Client interface for Python DB-API
    """


    def __init__(self, device=None, datacollector=None, plugins=[]):
        """
        Initializer

        @param device: remote device to use the datacollector
        @type device: device object
        @param datacollector: performance data collector object
        @type datacollector: datacollector object
        @param plugins: Python-based performance data collector plugin
        @type plugins: list of plugin objects
        """
        BaseClient.__init__(self, device, datacollector)
        self.device = device
        self.hostname = getattr(device, 'id', 'unknown')
        self.plugins = plugins
        self.results = []
        self._pools = getPool('adbapi executors')


    def query(self, queries):
        """
        Run SQL queries.

        @param queries: queries dictionary, with table name as a key and task
                        tuple as a value
        @type queries: dictionary
        """
        results = {}
        tasks = {}
        for table, task in queries.iteritems():
            tasks.setdefault(task[2], []).append((table, task))
        for cs, csTasks in tasks.iteritems():
            client = dbapiClient(cs)
            if not client.connect(): continue
            for table, task in csTasks:
                dsc = DataSourceConfig(*task)
                dsc.result = client.query(dsc)
                t,result = self.parseResult(dsc,'',table,client._dbapi.__name__)
                results[table] = result
            client.close()
            client = None
        return results


    def run(self):
        """
        Start SQL collection.
        """
        deferreds = []
        for plugin in self.plugins:
            log.debug("Running collection for plugin %s", plugin.name())
            tasks = []
            for table, task in plugin.prepareQueries(self.device).iteritems():
                dsc = DataSourceConfig(*task)
                dbapiName = dsc.connectionString.split(',', 1)[0].strip('\'"')
                executor = self._pools.setdefault(dbapiName, adbapiExecutor())
                deferred = executor.submit(dsc)
                deferred.addBoth(self.parseResult,plugin.name(),table,dbapiName)
                tasks.append(deferred)
            tdl = defer.DeferredList(tasks)
            deferreds.append(tdl)
            tdl.addBoth(self.collectComplete, plugin)
        dl = defer.DeferredList(deferreds)
        dl.addBoth(self.collectComplete, None)


    def parseResult(self, datasource, pName, table, dbapiName):
        """
        Twisted deferred callback used to store the
        results of the collection run

        @param r: result from the collection run
        @type r: result
        @param table: tables name
        @type table: string
        @param dbapiName: Python DB-API modules name
        @type dbapiName: string
        """
        if dbapiName in self._pools and self._pools[dbapiName]._running < 1:
            self._pools[dbapiName] = None
            del self._pools[dbapiName]
        if isinstance(datasource.result, Failure):
            log.warn('Error in %s query "%s": %s', pName, datasource.sql,
                                            datasource.result.getErrorMessage())
            return (table, datasource.result)
        log.debug('Results for %s query "%s": %s', pName, datasource.sql,
                                                        str(datasource.result))
        if datasource.points:
            r = [dict([(p.id,row.get(p.alias,'')) for p in datasource.points]) \
                                                for row in datasource.result]
        else:
            r = datasource.result
        return (table, r)


    def collectComplete(self, r, plugin):
        """
        Twisted deferred error callback used to store the
        results of the collection run

        @param r: result from the collection run
        @type r: result or Exception
        @param plugin: DBAPI-based performance data collector plugin
        @type plugin: plugin object
        """
        if plugin is None:
            self.clientFinished()
            return

        results = {}
        errors = 0
        errmsg = ''
        for success, (table, result) in r:
            if isinstance(result, Failure):
                results[table] = []
                errors += 1
                result.cleanFailure()
                errmsg = result.getErrorMessage()
            else:
                results[table] = result
        if len(r) == errors:
            results = Failure(errmsg)
        self.results.append((plugin, results))


    def clientFinished(self):
        """
        Stop the collection of performance data
        """
        log.info("SQL client finished collection for %s" % self.hostname)
        if self.datacollector:
            self.datacollector.clientFinished(self)

    def getResults(self):
        """
        Return the results of the data collection.
        To be implemented by child classes

        @return: list of results
        @rtype: list of results
        """
        return self.results

class SQLPlugin(object):

    def __init__(self, tables={}):
        self.tables = tables

    def prepareQueries(self, device=None):
        return self.tables

    def queries(self):
        tables = {}
        for tname, task in self.tables.iteritems():
            if len(task) == 4:
                sqlp, kbs, cs, columns = task
                sql = sqlp
            else:
                sqlp, kbs, cs, columns, sql = task
            tables[tname] = sql
        return tables

    def name(self):
        return ''

def sqlCollect(collector, device, ip, timeout):
    """
    Start sql collection client.

    @param collector: collector
    @type collector: string
    @param device: device to collect against
    @type device: string
    @param ip: IP address of device to collect against
    @type ip: string
    @param timeout: timeout before failing the connection
    @type timeout: integer
    """
    client = None
    try:
        plugins = collector.selectPlugins(device, "sql")
        if not plugins:
            collector.log.info("No SQL plugins found for %s" % device.id)
            return
        if collector.checkCollection(device):
            collector.log.info('SQL collection device %s' % device.id)
            collector.log.info("plugins: %s",
                    ", ".join(map(lambda p: p.name(), plugins)))
            client = SQLClient(device, collector, plugins)
        if not client or not plugins:
            collector.log.warn("SQL client creation failed")
            return
    except (SystemExit, KeyboardInterrupt): raise
    except:
        collector.log.exception("Error opening sql client")
    collector.addClient(client, timeout, 'SQL', device.id)

if __name__ == "__main__":
    cs = "'MySQLdb',host='127.0.0.1',port=3306,db='information_schema',user='zenoss',passwd='zenoss'"
    query = "USE information_schema; SHOW GLOBAL STATUS;"
    columns = ["Bytes_received", "Bytes_sent"]
    aliases = ["Bytes_received", "Bytes_sent"]

    logging.basicConfig()
    log = logging.getLogger()
    log.setLevel(20)

    import getopt
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hc:q:f:a:",
                    ["help", "cs=", "query=", "fields=", "aliases="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt in ("-h", "--help"):
            usage()
            sys.exit()
        elif opt in ("-c", "--cs"):
            cs = arg
        elif opt in ("-q", "--query"):
            query = arg
        elif opt in ("-f", "--fields"):
            columns = arg.split()
        elif opt in ("-a", "--aliases"):
            aliases = arg.split()
    columns = dict(zip(columns, aliases))
    queries = {'t': (query, {}, cs, columns)}
    cl = SQLClient(device=None)
    if 1:
        sp = SQLPlugin(queries)
        cl.plugins.append(sp)
        cl.clientFinished=reactor.stop
        cl.run()
        reactor.run()
        for plugin, results in cl.getResults():
            if plugin == sp: break
        else:
            results = {'t': Failure('ERROR:zen.SQLClient:No data received.')}
    else:
        try:
            results = cl.query(queries)
        except Exception, e:
            results = {'t':Failure(e)}
    if isinstance(results, Failure):
        print results.getErrorMessage()
        sys.exit(1)
    results = results.get('t', Failure('ERROR:zen.SQLClient:No data received.'))
    if isinstance(results, Failure):
        print results.getErrorMessage()
        sys.exit(1)
    if not columns:
        columns = dict(zip(results[0].keys(), results[0].keys()))
    print "|".join(columns.values())
    for row in results:
        print "|".join([str(row.get(dpname,'')) for dpname in columns.values()])

