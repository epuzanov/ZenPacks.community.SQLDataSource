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

$Id: SQLClient.py,v 2.20 2012/01/16 18:32:39 egor Exp $"""

__version__ = "$Revision: 2.20 $"[11:-2]

if __name__ == "__main__":
    from pysamba.twisted.reactor import reactor, eventContext
import Globals
from Products.ZenUtils.Utils import zenPath, unused
from Products.ZenUtils.Driver import drive
from Products.DataCollector.BaseClient import BaseClient

from twisted.enterprise import adbapi
from twisted.internet import defer
from twisted.python.failure import Failure
from pysamba.wbem.Query import Query
unused(Query)

import datetime
import decimal
from DateTime import DateTime

import re
DTPAT=re.compile(r'^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})\.(\d{6})([+|-])(\d{3})')

import os
import sys
import logging
log = logging.getLogger("zen.SQLClient")

BaseName = os.path.basename(sys.argv[0])
MyName = None


def _myname():
    global MyName
    if not MyName:
        MyName = BaseName.split('.')[0]
        try:
            os.mkdir(zenPath('var', _myname()))
        except os.error:
            pass
    return MyName

def _filename(device):
    return zenPath('var', _myname(), device)


class BadCredentials(Exception): pass


class asyncQuery(object):

    def __init__(self, sqlp, results, host):
        self.sql = ''
        self.sqlp = sqlp
        self.resMaps = {}
        self.results = results
        self.host = host

    def add(self, pname, tname, task):
        if len(task) == 4:
            sqlp, kbs, cs, columns = task
            sql = sqlp
        else:
            sqlp, kbs, cs, columns, sql = task
        if self.sql != self.sqlp: self.sql = self.sql and sqlp or sql
        table = ((pname, tname), columns) 
        ikey = tuple([str(k).lower() for k in (kbs or {}).keys()])
        ival = tuple([str(v).strip().lower() for v in (kbs or {}).values()])
        self.resMaps.setdefault(ikey, {}).setdefault(ival, []).append(table)

    def parseError(self, err):
        if isinstance(err, Failure):
            err.cleanFailure()
            err = err.getErrorMessage()
        else:
            err = getattr(err, 'value', err)
        log.error('Error received from %s, error: %s, query: %s'%(
                                                    self.host, err, self.sql))
        for instances in self.resMaps.values():
            for tables in instances.values():
                for (pname, table), props in tables:
                    self.results[pname][table] = Failure(err)
        return 1

    def parseValue(self, value):
        if isinstance(value, datetime.timedelta):
            return DateTime(datetime.datetime.now() - value)
        if isinstance(value, datetime.datetime): return DateTime(value)
        if isinstance(value, decimal.Decimal): return long(value)
        if type(value) not in (str, unicode): return value
        if value == 'false': return False
        if value == 'true': return True
        return value.strip()

    def parseResult(self, results):
        if not results: return 0
        rows = {}
        header = [h[0].lower() for h in results.pop(0)]
        for row in results:
            rDict = dict(zip(header, [self.parseValue(v) for v in row]))
            for kbKey, kbVal in self.resMaps.iteritems():
                cNames=set([k.lower() for k in kbVal.values()[0][0][1].keys()])
                if not cNames.intersection(set(header)):
                    rows[str(row[0]).lower()] = row[-1]
                    continue
                kIdx = []
                for kb in kbKey:
                    kbV = rDict.get(kb, '')
                    if kbV is list: kbV = ' '.join(kbV)
                    kIdx.append(str(kbV).lower())
                for (pn, table), cols in kbVal.get(tuple(kIdx), []):
                    result = {}
                    for name, alias in cols.iteritems():
                        result[alias] = rDict.get(name.lower(),'')
                    if result: self.results[pn][table].append(result)
        for kbVal in self.resMaps.values():
            for tables in kbVal.values():
                for (pn, table), cols in tables:
                    if self.results[pn][table]: continue
                    result = {}
                    for name, alias in cols.iteritems():
                        result[alias]=self.parseValue(rows.get(name.lower(),''))
                    if result: self.results[pn][table].append(result)
        return 0

    def execute(self, txn, sql):
        log.debug("Query: %s", sql)
        res = []
        for q in re.split('[ \n]go[ \n]|;[ \n]', sql, re.I):
            if not q.strip(): continue
            txn.execute(q.strip())
        res.extend(txn.fetchall())
        if res: res.insert(0, txn.description)
        return res

    def run(self, dbpool):
        d = dbpool.runInteraction(self.execute, self.sql)
        d.addCallback(self.parseResult)
        d.addErrback(self.parseError)
        return d


class asyncPool(object):

    poolType = 'Asynchronous'

    def __init__(self, cs):
        self.cs = cs
        self.queries = []
        self.pool = None
        self.connection = None

    def __del__(self):
        self.close()

    def close(self):
        if self.pool:
            self.pool.close()
        self.pool = None
        if self.connection:
            self.connection.close()
        self.connection = None
        if self.queries:
            self.cancel()

    def cancel(self, err=None):
        log.debug('%s pool %s is cancelled.'%(self.poolType, self))
        if not err:
            err = 'Query cancelled'
        errcount = 0
        while self.queries:
            query = self.queries.pop(0)
            errcount += query.parseError(err)
        return errcount

    def parseCS(self, cs=None):
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
                    else:
                        kwargs[var.strip()] = int(val.strip())
                except: args.append(arg)
        return args, kwargs

    def add(self, pname, tname, task, results, host):
        sqlp = task[0]
        for query in self.queries:
            if query.sqlp != sqlp: continue
            query.add(pname, tname, task)
            return
        self.queries.append(asyncQuery(task[0], results, host))
        self.queries[-1].add(pname, tname, task)

    def connect(self):
        args, kwargs = self.parseCS(self.cs)
        kwargs.update({'cp_min':1,'cp_max':1})
        self.pool = adbapi.ConnectionPool(*args, **kwargs)
        return defer.succeed(None)

    def run(self):
        def inner(driver):
            log.debug('%s pool %s is running.'%(self.poolType, self))
            qlen = len(self.queries)
            errcount = 0
            try:
                yield self.connect()
                driver.next()
            except Failure, e:
                e.cleanFailure()
                errcount += self.cancel(e.getErrorMessage())
            except Exception, e:
                errcount += self.cancel(str(e))
            while self.queries:
                query = self.queries.pop(0)
                yield query.run(self.pool)
                errcount += driver.next()
            self.close()
            yield defer.succeed(errcount == qlen and 1 or 0)
            driver.next()
        return drive(inner)


class syncPool(asyncPool):

    poolType = 'Synchronous'

    def connect(self):
        args, kwargs = self.parseCS(self.cs)
        fl = []
        if '.' in args[0]: fl.append(args[0].split('.')[-1])
        dbapi = __import__(args[0], globals(), locals(), fl)
        self.connection = dbapi.connect(*args[1:], **kwargs)
        self.pool = self.connection.cursor()

    def run(self):
        log.debug('%s pool %s is running.'%(self.poolType, self))
        qlen = len(self.queries)
        errcount = 0
        try:
            self.connect()
        except Exception, e:
            errcount += self.cancel(str(e))
        while self.queries:
            query = self.queries.pop(0)
            try:
                res = query.execute(self.pool, query.sql)
                errcount += query.parseResult(res)
            except Exception, e:
                errcount += query.parseError(e)
        self.close()
        return errcount == qlen and 1 or 0


class wmiQuery(asyncQuery):

    def parseResult(self, instances):
        for instance in instances:
            for kbKey, kbVal in self.resMaps.iteritems():
                kIdx = []
                for kb in kbKey:
                    kbV = getattr(instance, kb, '')
                    if kbV is list: kbV = ' '.join(kbV)
                    kIdx.append(str(kbV).lower())
                for (pn, table), properties in kbVal.get(tuple(kIdx), []):
                    result = {}
                    if len(properties) == 0:
                        properties = instance.__dict__.keys()
                    if type(properties) is not dict:
                        properties = dict(zip(properties, properties))
                    for name, alias in properties.iteritems():
                        if name is '_class_name': continue
                        res = getattr(instance, name.lower(), None)
                        if type(res) is str:
                            r = DTPAT.search(res)
                            if r:
                                g = r.groups()
                                if g[8] == '000':
                                    tz = 'GMT'
                                else:
                                    hr, mn = divmod(int(g[8]), 60)
                                    if 0 < mn < 1: mn = mn * 60
                                    tz = 'GMT%s%02d%02d' % (g[7], hr, mn)
                                res = DateTime(int(g[0]), int(g[1]), int(g[2]),
                                                int(g[3]),int(g[4]),
                                                float('%s.%s'%(g[5],g[6])), tz)
                        result[alias] = res
                    if result: self.results[pn][table].append(result)
        return 0

    def run(self, pool):
        def inner(driver):
            errcount = 0
            query = self.sql
            log.debug("WMI Query: %s", query)
            try:
                yield pool.query(query)
                result = driver.next()
                while 1:
                    more = None
                    yield result.fetchSome()
                    more = driver.next()
                    if not more: break
                    self.parseResult(more)
            except Exception, ex:
                if str(ex) not in (
                    "NT code 0x80041010",
                    "WBEM_E_INVALID_CLASS",
                    ): pass
                errcount = 1
                self.parseError(ex)
            yield defer.succeed(errcount)
            driver.next()
        return drive(inner)

class wmiPool(asyncPool):

    poolType = 'pysamba'

    def add(self, pname, tname, task, results, host):
        sqlp = task[0]
        for query in self.queries:
            if query.sqlp != sqlp: continue
            query.add(pname, tname, task)
            return
        self.queries.append(wmiQuery(task[0], results, host))
        self.queries[-1].add(pname, tname, task)

    def connect(self):
        args, kwargs = self.parseCS(self.cs)
        host = kwargs.get('host', 'localhost')
        user = kwargs.get('user', '')
        if not user:
            log.warning("Windows login name is unset: "
                        "please specify zWinUser and "
                        "zWinPassword zProperties before adding devices.")
            raise BadCredentials("Username is empty")
        password = kwargs.get('password', '')
        creds = '%s%%%s'%(user, password)
        namespace = kwargs.get('namespace', 'root/cimv2')
        log.debug("connect to %s, user %s", host, user)
        self.pool = Query()
        return self.pool.connect(eventContext, host, host, creds, namespace)


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
        return 'SQLPlugin'


class SQLClient(BaseClient):

    def __init__(self, device=None, datacollector=None, plugins=[]):
        BaseClient.__init__(self, device, datacollector)
        self.device = device
        self.host = getattr(device, 'id', 'unknown')
        self.datacollector = datacollector
        self.plugins = plugins
        self.results = []
        self._queue = {}

    def __del__(self):
        self.close()

    def close(self):
        del self.plugins[:]
        del self.results[:]
        while self._queue:
            cs, pool = self._queue.popitem()
            pool.close()

    def query(self, tasks={}, sync=True):
        results = {}
        for tname, task in tasks.iteritems():
            if type(tname) is tuple: pname, tname = tname
            else: pname = None
            results.setdefault(pname, {})[tname] = []
            cs = task[2]
            if sync:
                pool = self._queue.setdefault(cs, syncPool(cs))
            elif task[2].startswith("'pywmidb'"):
                pool = self._queue.setdefault(cs, syncPool(cs))
            else:
                pool = self._queue.setdefault(cs, asyncPool(cs))
            pool.add(pname, tname, task, results, self.host)
        if sync:
            errcount = 0
            qlen = len(self._queue)
            while self._queue:
                cs, pool = self._queue.popitem()
                try: errcount += pool.run()
                except Exception, e: errcount += 1
                pool.close()
            if errcount == qlen:
                results.values()[0].values()[0].raiseException()
            return results.pop(None, results)
        def inner(driver):
            errcount = 0
            qlen = len(self._queue)
            while self._queue:
                cs, pool = self._queue.popitem()
                try:
                    yield defer.maybeDeferred(pool.run)
                    errcount += driver.next()
                except Exception, e: errcount += 1
                pool.close()
            if errcount == qlen:
                results[None] = results.values()[0].values()[0]
            yield defer.succeed(results.pop(None, results))
            driver.next()
        return drive(inner)

    def run(self):
        def inner(driver):
            tasks = {}
            for plugin in self.plugins:
                pn = plugin.name()
                for tn,t in (plugin.prepareQueries(self.device) or {}).iteritems():
                    tasks[(pn,tn)] = t
            yield defer.maybeDeferred(self.query, tasks, sync=False)
            driver.next()
        d = drive(inner)
        def finish(results):
            if isinstance(results, Failure):
                for pl in self.plugins:
                    self.results.append((pl, results))
            else:
                for pl in self.plugins:
                    self.results.append((pl, results.pop(pl.name(), {})))
            if self.datacollector:
                self.datacollector.clientFinished(self)
            else:
                reactor.stop()
        d.addBoth(finish)
        return d

    def getResults(self):
        """Return data for this client
        """
        return self.results


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
    if 0:
        sp = SQLPlugin({'t': (query, {}, cs, columns)})
        cl.plugins.append(sp)
        cl.run()
        reactor.run()
        results = Failure('ERROR:zen.SQLClient:No data received.')
        for plugin, result in cl.getResults():
            if plugin != sp: continue
            results = result
            break
        sp = None
    else:
        try: results = cl.query({'t':(query, {}, cs, columns)})
        except Exception, e: results = Failure(e)
    cl.close()
    if isinstance(results, Failure):
        print results.getErrorMessage()
        sys.exit(1)
    results = results.get('t', [])
    if not results:
        print 'ERROR:zen.SQLClient:No data received.'
        sys.exit(1)
    if not columns:
        columns = dict(zip(results[0].keys(), results[0].keys()))
    print "|".join(columns.values())
    for row in results:
        print "|".join([str(row.get(dpname,'')) for dpname in columns.values()])
