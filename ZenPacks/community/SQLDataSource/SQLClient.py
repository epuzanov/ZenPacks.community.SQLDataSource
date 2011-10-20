################################################################################
#
# This program is part of the SQLDataSource Zenpack for Zenoss.
# Copyright (C) 2009, 2010, 2011 Egor Puzanov.
#
# This program can be used under the GNU General Public License version 2
# You can find full information here: http://www.zenoss.com/oss
#
################################################################################

__doc__="""SQLClient

Gets performance data over python DB API.

$Id: SQLClient.py,v 2.7 2011/10/20 18:43:56 egor Exp $"""

__version__ = "$Revision: 2.7 $"[11:-2]

import Globals
from Products.ZenUtils.Utils import zenPath
from Products.ZenUtils.Driver import drive
from Products.DataCollector.BaseClient import BaseClient

from twisted.enterprise import adbapi
from twisted.internet import defer, reactor
from twisted.python.failure import Failure

import datetime
import decimal
from DateTime import DateTime

import re
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


class Query(object):

    def __init__(self, cs, client=None):
        self.cs = cs 
        self.sql = ''
        self.resMaps = {}

    def add(self, pname, tname, task):
        if len(task) == 4:
            sqlp, kbs, cs, columns = task
            sql = sqlp
        else:
            sqlp, kbs, cs, columns, sql = task
        if self.sql != sqlp:
            if not self.sql: self.sql = sql
            else: self.sql = sqlp
        table = ((pname, tname), columns) 
        ikey = tuple([str(k).upper() for k in (kbs or {}).keys()])
        ival = tuple([str(v).strip().upper() for v in (kbs or {}).values()])
        self.resMaps.setdefault(ikey, {}).setdefault(ival, []).append(table)


    def parseError(self, err):
        results = {}
        err = Failure(err)
        err.value = 'Received error (%s) from query: %s'%(err.value, self.sql)
        log.error(err.getErrorMessage())
        for instances in self.resMaps.values():
            for tables in instances.values():
                for (pname, table), props in tables:
                    results.setdefault(pname,{})[table] = [err,]
        return results


    def parseValue(self, value):
        if isinstance(value, datetime.timedelta):
            return DateTime(datetime.datetime.now() - value)
        if isinstance(value, datetime.datetime): return DateTime(value)
        if isinstance(value, decimal.Decimal): return long(value)
        if type(value) not in (str, unicode): return value
        if value.isdigit(): return long(value)
        if value.replace('.', '', 1).isdigit(): return float(value)
        if value == 'false': return False
        if value == 'true': return True
        return value.strip()


    def parseResult(self, cursor):
        manyrows = []
        rows = {}
        results = {}
        if not cursor.description:
            for instances in resMaps.values():
                for tables in instances.values():
                    for (pn, table), props in tables:
                        results.setdefault(pn, {})[table] = []
            return results
        header = [h[0].upper() for h in cursor.description]
        manyrows.extend(cursor.fetchmany())
        while manyrows:
            while manyrows:
                row = manyrows.pop(0)
                rDict = dict(zip(header, [self.parseValue(v) for v in row]))
                for kbKey, kbVal in self.resMaps.iteritems():
                    cNames=set([k.upper() for k in kbVal.values()[0][0][1].keys()])
                    if not cNames.intersection(set(header)):
                        rows[str(row[0]).upper()] = row[-1]
                        continue
                    kIdx = []
                    for kb in kbKey:
                        kbV = rDict.get(kb, '')
                        if kbV is list: kbV = ' '.join(kbV)
                        kIdx.append(str(kbV).upper())
                    for (pn, table), cols in kbVal.get(tuple(kIdx), []):
                        result = {}
                        for name, anames in cols.iteritems():
                            if not hasattr(anames, '__iter__'): anames=(anames,)
                            for aname in anames:
                                result[aname] = rDict.get(name.upper(), None)
                        if result: results.setdefault(pn, {}).setdefault(
                                                        table,[]).append(result)
            manyrows.extend(cursor.fetchmany())
        for kbVal in self.resMaps.values():
            for tables in kbVal.values():
                for (pn, table), cols in tables:
                    if table in results.get(pn, {}): continue
                    result = {}
                    for name, anames in cols.iteritems():
                        val = self.parseValue(rows.get(name.upper(), None))
                        if not hasattr(anames, '__iter__'): anames=(anames,)
                        for aname in anames: result[aname] = val
                    if result: results.setdefault(pn, {}).setdefault(
                                                        table,[]).append(result)
        return results


    def run(self, dbpool):
        def _execute(txn):
            for q in re.split('[ \n]go[ \n]|;[ \n]', self.sql, re.I):
                if not q.strip(): continue
                txn.execute(q.strip())
            return self.parseResult(txn)
        if not hasattr(dbpool, 'runInteraction'):
            try: return _execute(dbpool)
            except Exception, e: return self.parseError(e) 
        d = dbpool.runInteraction(_execute)
        d.addErrback(self.parseError)
        return d


class SQLPlugin(object):

    def __init__(self, tables={}):
        self.tables = tables

    def prepareQueries(self, device=None):
        return self.tables

    def name(self):
        return 'SQLPlugin'


class SQLClient(BaseClient):

    def __init__(self, device=None, datacollector=None, plugins=[]):
        BaseClient.__init__(self, device, datacollector)
        self.device = device
        self.datacollector = datacollector
        self.plugins = plugins
        self.results = []


    def __del__(self):
        self.close()


    def getDbPool(self, cs=None):
        if not cs: return None
        args, kwargs = self.parseCS(cs)
        kwargs.update({'cp_min':1,'cp_max':1})
        return adbapi.ConnectionPool(*args,**kwargs)


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


    def close(self):
        del self.plugins[:]
        del self.results[:]


    def syncQuery(self, tasks={}):
        results = {}
        queue = {}
        for (pn, tn), q in tasks.iteritems():
            queue.setdefault(q[2],{}).setdefault(q[0],Query(q[2])).add(pn,tn,q)
        for cs, qlist in queue.iteritems():
            args, kwargs = self.parseCS(cs)
            fl = []
            if '.' in args[0]: fl.append(args[0].split('.')[-1])
            try:
                dbapi = __import__(args[0], globals(), locals(), fl)
                connection = dbapi.connect(*args[1:], **kwargs)
                dbcursor = connection.cursor()
                for sql, query in qlist.iteritems():
                    log.debug("SQL Query: %s", sql)
                    result = query.run(dbcursor)
                    for pn, res in result.iteritems():
                        results.setdefault(pn, {}).update(res)
                dbcursor.close()
                connection.close()
            except Exception, e:
                for sql, query in qlist.iteritems():
                    result = query.parseError(e)
                    for pn, res in result.iteritems():
                        results.setdefault(pn, {}).update(res)
        return results


    def runPool(self, cs, qlist):
        def inner(driver):
            results = {}
            dbpool = self.getDbPool(cs)
            for sql, query in qlist.iteritems():
                log.debug("SQL Query: %s", sql)
                yield query.run(dbpool)
                result = driver.next()
                for pn, res in result.iteritems():
                    results.setdefault(pn, {}).update(res)
            dbpool.close()
            yield defer.succeed(results)
            driver.next()
        return drive(inner)


    def query(self, tasks={}):
        queue = {}
        for (pn, tn), q in tasks.iteritems():
            queue.setdefault(q[2],{}).setdefault(q[0],Query(q[2])).add(pn,tn,q)
        def inner(driver):
            results = {}
            for cs, queries in queue.iteritems():
                yield self.runPool(cs, queries)
                result = driver.next()
                for pn, res in result.iteritems():
                    results.setdefault(pn, {}).update(res)
            yield defer.succeed(results)
            driver.next()
        return drive(inner)


    def run(self):
        def finish(result):
            for plugin in self.plugins:
                self.results.append((plugin, result.get(plugin.name(), None)))
            if self.datacollector:
                self.datacollector.clientFinished(self)
            else:
                reactor.stop()
        tasks = {}
        for plugin in self.plugins:
            pn = plugin.name()
            for tn,t in (plugin.prepareQueries(self.device) or {}).iteritems():
                tasks[(pn,tn)] = t
        d = self.query(tasks)
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


def SQLGet(cs, query, columns):
    sp = SQLPlugin({'t': (query, {}, cs, columns)})
    cl = SQLClient(device=None, plugins=[sp,])
    cl.run()
    reactor.run()
    err = [Failure('ERROR:zen.SQLClient:No data received.')]
    for plugin, result in cl.getResults():
        if plugin == sp: return result.get('t', err)
    return err


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
    results = SQLGet(cs, query, columns)
#    cl = SQLClient()
#    results = cl.syncQuery({(None,'t'):(query, {}, cs, columns)}).get(None, {}).get('t',
#                            [Failure('ERROR:zen.SQLClient:No data received.')])
    if isinstance(results[0], Failure):
        print results[0].getErrorMessage()
        sys.exit(1)
    if not columns:
        columns = dict(zip(results[0].keys(), results[0].keys()))
    print "|".join(columns.values())
    for row in results:
        print "|".join([str(row.get(dpname,'')) for dpname in columns.values()])
