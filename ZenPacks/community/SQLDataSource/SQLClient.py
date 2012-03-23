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

$Id: SQLClient.py,v 3.0 2012/03/15 19:00:45 egor Exp $"""

__version__ = "$Revision: 3.0 $"[11:-2]

import logging
log = logging.getLogger("zen.SQLClient")

import Globals

from Products.DataCollector.BaseClient import BaseClient
from twisted.internet.defer import Deferred, DeferredList
from twisted.python.failure import Failure
from twisted.enterprise import adbapi
from twisted.internet import reactor

from threading import Timer
import sys
import re

try:
    from Products.ZenCollector.pools import getPool
except:
    ADBAPIPOOLS = {}
    def getPool(name, factory=None):
        return ADBAPIPOOLS

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

def runQuery(txn, sql, columns, dbapi, timeout, _timeout):
    def _convert(val, type):
        if val is None:
            if type == dbapi.STRING:
                return ''
            if type == dbapi.NUMBER:
                return 0
            return None
        if type == dbapi.NUMBER:
            return float(val)
        if type == dbapi.STRING:
            return str(val).strip()
        return val
    res = []
    t = Timer(timeout, _timeout, txn)
    t.start()
    try:
        for q in re.split('[ \n]go[ \n]|;[ \n]', sql, re.I):
            if not q.strip(): continue
            txn.execute(q.strip())
    except Exception, ex:
        if t.isAlive():
            t.cancel()
        else:
            ex = TimeoutError('Query Timeout')
        raise ex
    t.cancel()
    header, ct = zip(*[(h[0].lower(), h[1]) for h in txn.description or []])
    if not header: return res
    if columns.intersection(set(header)):
        varVal = False
    else:
        res.append({})
        varVal = True
    rows = txn.fetchmany()
    while rows:
        for row in rows:
            if varVal:
                res[0][str(row[0]).lower()] = _convert(row[-1], ct[-1])
            else:
                res.append(dict(zip(header,[_convert(*v) for v in zip(row,ct)])))
        rows = txn.fetchmany()
    return res


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

    def submit(self, cs, name, sql, columns={}, keybindings={}, timeout=20):
        """
        submit a query to be executed. A deferred will be returned with the
        the result of the query.

        @param cs: connection string
        @type cs: string
        @param name: tasks name (parsed sql query)
        @type name: string
        @param sql: sql query
        @type sql: string
        @param columns: columns to return
        @type columns: dict
        @param keybindings: filter
        @type keybindings: dict
        @param timeout: timeout in seconds
        @type timeout: integer
        """
        deferred = Deferred()
        deferred.addBoth(self._taskFinished)
        for task in self._taskQueue:
            if task == name: break
        else:
            task = adbapiTask(cs, name, sql, timeout)
            self._taskQueue.append(task)
        task.addTask(deferred, columns, keybindings)
        reactor.callLater(2, self._runTask)
        return deferred

    def _runTask(self):
        if self._taskQueue and self._running < self._max:
            if not self._connection:
                task = self._taskQueue.pop(0)
                args, kwargs = parseConnectionString(task._cs)
                connection = adbapi.ConnectionPool(*args, **kwargs)
                self._cs = str(task._cs)
                self._connection = connection
            else:
                for taskId, task in enumerate(self._taskQueue):
                    if task._cs == self._cs: break
                else:
                    taskId = 0
                    self._taskQueue[taskId]
                    args, kwargs = parseConnectionString(task._cs)
                    connection = adbapi.ConnectionPool(*args, **kwargs)
                    self._cs = str(task._cs)
                    self._connection = connection
                task = self._taskQueue.pop(taskId)
            self._running += len(task._deferreds)
            task(self._connection)
            reactor.callLater(0, self._runTask)

    def _taskFinished(self, result):
        if self._running > 0:
            self._running -= 1
        if not self._taskQueue and self._running < 1:
            self._connection.close()
            self._connection = None
            self._cs = None
        reactor.callLater(0, self._runTask)
        return result

class adbapiTask(object):
    """
    Used by adbapiExecutor to execute queued query
    """
    def __init__(self, cs, name, sql, timeout):
        self._cs = cs
        self._name = name
        self._sql = sql
        self._columns = set()
        self._timeout = timeout
        self._deferreds = []

    def __cmp__(self, other):
        return cmp(self._name, other)

    def __repr__(self):
        return self._name

    def __call__(self, pool):
        def _timeout(txn):
            if hasattr(txn._connection, 'cancel'):
                txn._connection.cancel()
            txn._cursor.close()
        if len(self._deferreds) > 1:
            self._sql = self._name
        deferred = pool.runInteraction( runQuery, self._sql, self._columns,
                                        pool.dbapi, self._timeout, _timeout)
        deferred.addCallbacks(self._finished, self._error)

    def _finished(self, results):
        for deferred, kc, kv in self._deferreds:
            if not kc:
                deferred.callback(results)
                continue
            result = []
            for row in results:
                if kv==''.join([(row.get(k) or '').strip() for k in kc]).lower():
                    result.append(row)
            deferred.callback(result)

    def _error(self, results):
        results.cleanFailure()
        for deferred, kc, kv in self._deferreds:
            deferred.errback(results.getErrorMessage())

    def addTask(self, deferred, columns={}, keybindings={}):
        if columns:
            self._columns.update(map(lambda v: v.lower(), columns.values()))
        if keybindings:
            kc, kv = zip(*[map(lambda v: str(v).strip().lower(), k) \
                                            for k in keybindings.iteritems()])
        else: kc, kv = (), ()
        self._deferreds.append((deferred, kc, ''.join(kv)))


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


    def query(self, queries, sync=True, plugin=''):
        """
        Run SQL queries.
        """

        def _finished(r):
            results = {}
            errors = 0
            for success, (table, result) in r:
                if isinstance(result, Failure):
                    results[table] = []
                    errors += 1
                    result.cleanFailure()
                else:
                    results[table] = result
            if errors and len(r) == errors:
                results = Failure(result.getErrorMessage())
            if sync:
                self.results.append(('', results))
                reactor.stop()
            else:
                return results
        deferreds = []
        for table, task in queries.iteritems():
            if len(task) == 4:
                sqlp, kbs, cs, columns = task
                sql = sqlp
            else:
                sqlp, kbs, cs, columns, sql = task
            dbapiName = cs.split(',', 1)[0].strip('\'"')
            executor = self._pools.setdefault(dbapiName, adbapiExecutor())
            deferred = executor.submit(cs, sqlp, sql, columns, kbs)
            deferred.addCallback(self.parseResult, plugin, sql, columns)
            deferred.addErrback(self.parseError, plugin, sql, cs)
            deferred.addBoth(self.addResult, table, dbapiName)
            deferreds.append(deferred)
        dl = DeferredList(deferreds)
        dl.addCallback(_finished)
        if sync:
            reactor.run()
            return self.results.pop(0)[1]
        else:
            return dl

    def run(self):
        """
        Start SQL collection.
        """
        deferreds = []
        for plugin in self.plugins:
            log.debug("Running collection for plugin %s", plugin.name())
            tasks = []
            for table, task in plugin.prepareQueries().iteritems():
                if len(task) == 4:
                    sqlp, kbs, cs, columns = task
                    sql = sqlp
                else:
                    sqlp, kbs, cs, columns, sql = task
                dbapiName = cs.split(',', 1)[0].strip('\'"')
                executor = self._pools.setdefault(dbapiName, adbapiExecutor())
                deferred = executor.submit(cs, sqlp, sql, columns, kbs)
                deferred.addCallback(self.parseResult, plugin.name(), sql, columns)
                deferred.addErrback(self.parseError, plugin.name(), sql, cs)
                deferred.addBoth(self.addResult, table, dbapiName)
                tasks.append(deferred)
            tdl = DeferredList(tasks)
            deferreds.append(tdl)
            tdl.addBoth(self.collectComplete, plugin)
        dl = DeferredList(deferreds)
        dl.addBoth(self.collectComplete, None)


    def parseResult(self, r, pName, sql, columns):
        """
        Twisted deferred callback used to store the
        results of the collection run

        @param r: result from the collection run
        @type r: result
        @param pName: Name of performance data collector plugin
        @type pName: plugin string
        @param sql: SQL query
        @type sql: string
        @param columns: columns-aliases dictionary
        @type columns: dict
        """
        log.debug('Results for %s query "%s": %s', pName, sql, str(r))
        if columns:
            columns = dict(zip(columns.values(), columns.keys()))
            r = [dict(zip(columns.values(), tuple([row.get(cn.lower(), '') \
                for cn in columns.keys()]))) for row in r]
        return r


    def parseError(self, r, pName, sql, cs):
        """
        Twisted deferred error callback display errors

        @param r: result from the collection run
        @type r: result
        @param pName: Name of performance data collector plugin
        @type pName: plugin string
        @param sql: SQL query
        @type sql: string
        @param cs: connection string
        @type cs: string
        """
        log.debug('ConnectionString: %s', cs)
        log.warn('Error in %s query "%s": %s',pName,sql,r.getErrorMessage())
        return r


    def addResult(self, r, table, dbapiName):
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
        for success, (table, result) in r:
            if isinstance(result, Failure):
                results[table] = []
                errors += 1
                result.cleanFailure()
            else:
                results[table] = result
        if len(r) == errors:
            results = Failure(result.getErrorMessage())
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
    cl = SQLClient(device=None)
    if 1:
        sp = SQLPlugin({'t': (query, {}, cs, columns)})
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
            results = cl.query({'t':(query, {}, cs, columns)})
        except Exception, e:
            results = {'t', Failure(e)}
    results = results.get('t', Failure('ERROR:zen.SQLClient:No data received.'))
    if isinstance(results, Failure):
        print results.getErrorMessage()
        sys.exit(1)
    if not columns:
        columns = dict(zip(results[0].keys(), results[0].keys()))
    print "|".join(columns.values())
    for row in results:
        print "|".join([str(row.get(dpname,'')) for dpname in columns.values()])
