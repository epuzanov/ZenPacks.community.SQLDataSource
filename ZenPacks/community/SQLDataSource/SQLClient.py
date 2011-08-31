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

$Id: SQLClient.py,v 2.5 2011/08/31 19:09:24 egor Exp $"""

__version__ = "$Revision: 2.5 $"[11:-2]

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


class SQLClient(BaseClient):

    def __init__(self, device=None, datacollector=None, plugins=[]):
        BaseClient.__init__(self, device, datacollector)
        self.device = device
        self.datacollector = datacollector
        self.plugins = plugins
        self.results = []


    def makePool(self, cs=None):
        if not cs: return None
        args, kwargs = self.parseCS(cs)
        kwargs.update({'cp_min':1,'cp_max':1})
        return adbapi.ConnectionPool(*args, **kwargs)


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


    def parseError(self, err, query, resMaps):
        err = Failure(err)
        err.value = 'Received error (%s) from query: %s'%(err.value, query)
        log.error(err.getErrorMessage())
        results = {}
        for instances in resMaps.values():
            for tables in instances.values():
                for table, props in tables:
                    results[table] = [err,]
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


    def parseResults(self, cursor, resMaps):
        results = {}
        rows = {}
        if not cursor.description:
            for instances in resMaps.values():
                for tables in instances.values():
                    for table, props in tables:
                        results[table] = []
            return results
        header = [h[0].upper() for h in cursor.description]
        for row in (hasattr(cursor,'__iter__') and cursor or cursor.fetchall()):
            rDict = dict(zip(header, [self.parseValue(v) for v in row]))
            for kbKey, kbVal in resMaps.iteritems():
                cNames=set([k.upper() for k in kbVal.values()[0][0][1].keys()])
                if not cNames.intersection(set(header)):
                    rows[str(row[0]).upper()] = row[-1]
                    continue
                kIdx = []
                for kb in kbKey:
                    kbV = rDict.get(kb, '')
                    if kbV is list: kbV = ' '.join(kbV)
                    kIdx.append(str(kbV).upper())
                for table, cols in kbVal.get(tuple(kIdx), []):
                    if table not in results: results[table] = []
                    result = {}
                    for name, anames in cols.iteritems():
                        if not hasattr(anames, '__iter__'): anames=(anames,)
                        for aname in anames:
                            result[aname] = rDict.get(name.upper(), None)
                    if result: results[table].append(result)
        for kbVal in resMaps.values():
            for tables in kbVal.values():
                for table, cols in tables:
                    if table in results: continue
                    results[table] = []
                    result = {}
                    for name, anames in cols.iteritems():
                        val = self.parseValue(rows.get(name.upper(), None))
                        if not hasattr(anames, '__iter__'): anames=(anames,)
                        for aname in anames: result[aname] = val
                    if result: results[table].append(result)
        return results


    def close(self):
        del self.results[:]
        del self.plugins[:]


    def sortQueries(self, tasks={}):
        qIdx = {}
        queries = {}
        for tn, task in tasks.iteritems():
            if len(task) == 4:
                sql, kbs, cs, columns = task
                sqlp = sql
            else:
                sql, sqlp, kbs, cs, columns = task
            table = (tn, columns)
            ikey = tuple([str(k).upper() for k in (kbs or {}).keys()])
            ival = tuple([str(v).strip().upper() for v in (kbs or {}).values()])
            if cs not in queries:
                queries[cs] = {}
                qIdx[cs] = {}
            if sqlp not in queries[cs]:
                if sqlp in qIdx[cs]:
                    queries[cs][sqlp] = qIdx[cs][sqlp][1]
                    del queries[cs][qIdx[cs][sqlp][0]]
                else:
                    qIdx[cs][sqlp] = (sql, {ikey:{ival:[table]}})
                    queries[cs][sql]={():{():[table]}}
                    continue
            if ikey not in queries[cs][sqlp]:
                queries[cs][sqlp][ikey] = {ival:[table]}
            elif ival not in queries[cs][sqlp][ikey]:
                queries[cs][sqlp][ikey][ival] = [table]
            else:
                queries[cs][sqlp][ikey][ival].append(table)
        qIdx = None
        return queries


    def syncQuery(self, tasks={}):
        results = {}
        for cs, qs in self.sortQueries(tasks).iteritems():
            args, kwargs = self.parseCS(cs)
            dbapi = __import__(args[0], globals(), locals(), '')
            connection = dbapi.connect(*args[1:], **kwargs)
            dbcursor = connection.cursor()
            for sql, resMaps in qs.iteritems():
                qList = [q.strip() for q in re.split('[ \n]go[ \n]|;[ \n]',
                                                sql, re.I) if q.strip()]
                try:
                    for q in qList[:-1]:
                        dbcursor.execute(q)
                    dbcursor.execute(qList[-1])
                    results.update(self.parseResults(dbcursor, resMaps))
                except StandardError, ex:
                    results.update(self.parseError(ex, sql, resMaps))
            dbcursor.close()
            connection.close()
        return results


    def query(self, tasks={}):
        return self.sortedQuery(self.sortQueries(tasks))


    def sortedQuery(self, queries):
        def inner(driver):
            err = True
            queryResult = {}
            for cs, qs in queries.iteritems():
                yield self.queryPoll(cs, qs)
                queryResult.update(driver.next())
            for val in queryResult.values():
                if not isinstance(val[0], Failure):
                    err = False
                    break
            if err: raise Exception(val[0].getErrorMessage())
            yield defer.succeed(queryResult)
            driver.next()
        return drive(inner)


    def queryPoll(self, cs, qs):
        def _execute(txn, sql, resMaps):
            qList = [q.strip() for q in re.split('[ \n]go[ \n]|;[ \n]',
                                                sql, re.I) if q.strip()]
            for q in qList[:-1]:
                txn.execute(q)
#                txn.fetchall()
            txn.execute(qList[-1])
            return self.parseResults(txn, resMaps)
        def inner(driver):
            qResult = {}
            dbpool = self.makePool(cs)
            for query, resMaps in qs.iteritems():
                log.debug("SQL Query: %s", query)
                try:
                    yield dbpool.runInteraction(_execute, query, resMaps)
                    qResult.update(driver.next())
                except StandardError, ex:
                    qResult.update(self.parseError(ex, query, resMaps))
            yield defer.succeed(qResult)
            driver.next()
            dbpool.close()
        return drive(inner)


    def run(self):
        def inner(driver):
            queries = {}
            results = {}
            for plugin in self.plugins:
                pluginName = plugin.name()
                log.debug("Sending queries for plugin: %s", pluginName)
                log.debug("Queries: %s" % str(plugin.queries(self.device)))
                for tn, q in plugin.prepareQueries(self.device).iteritems():
                    queries["%s/%s"%(pluginName, tn)] = q
            yield self.query(queries)
            for tn, res in driver.next().iteritems():
                pn, oldtn = tn.split('/', 1)
                if pn not in results: results[pn] = {}
                results[pn][oldtn] = res
            for plugin in self.plugins:
                self.results.append((plugin, results.get(plugin.name(), None)))
        d = drive(inner)
        def finish(result):
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
    cl = SQLClient()
    results = cl.syncQuery({'t':(query, {}, cs, columns)}).get('t',
                            [Failure('ERROR:zen.SQLClient:No data received.')])
    if isinstance(results[0], Failure):
        print results[0].getErrorMessage()
        sys.exit(1)
    if not columns:
        columns = dict(zip(results[0].keys(), results[0].keys()))
    print "|".join(columns.values())
    for row in results:
        print "|".join([str(row.get(dpname,'')) for dpname in columns.values()])
