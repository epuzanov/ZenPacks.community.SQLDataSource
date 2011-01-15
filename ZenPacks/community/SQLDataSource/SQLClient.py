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

$Id: SQLClient.py,v 1.5 2011/01/15 12:35:41 egor Exp $"""

__version__ = "$Revision: 1.5 $"[11:-2]

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


def sortQuery(qs, table, query):
    sql, kbs, cs, cols = query
    if not kbs: kbs = {}
    ikey = tuple(kbs.keys())
    ival = tuple(kbs.values())
    try:
        if ival not in qs[cs][sql][ikey]:
            qs[cs][sql][ikey][ival] = []
        qs[cs][sql][ikey][ival].append((table, cols))
    except KeyError:
        try:
            qs[cs][sql][ikey] = {}
        except KeyError:
            try:
                qs[cs][sql] = {}
            except KeyError:
                qs[cs] = {}
                qs[cs][sql] = {}
            qs[cs][sql][ikey] = {}
        qs[cs][sql][ikey][ival] = [(table, cols)]
    return qs


class SQLClient(BaseClient):

    def __init__(self, device=None, datacollector=None, plugins=[]):
        BaseClient.__init__(self, device, datacollector)
        self.device = device
        self.datacollector = datacollector
        self.plugins = plugins
        self.results = []
        self._dbpool = None


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
        if isinstance(value, datetime.datetime): return DateTime(value)
        if isinstance(value, decimal.Decimal): return long(value)
        if value == None: return None
        try: return int(value)
        except: pass
        try: return float(value)
        except: pass
        try: return DateTime(value)
        except: pass
        try: return value.strip()
        except: return value


    def makePool(self, cs=None):
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
        return adbapi.ConnectionPool(*args, **kwargs)


    def close(self):
        if hasattr(self._dbpool, 'close'):
            self._dbpool.close()
        self._dbpool = None


    def parseResults(self, rows, resMaps):
        results = {}
        try:
            header = [h[0].upper() for h in rows[0]]
            columns = zip(*rows[1])
        except: return results
        for kbKey, kbVal in resMaps.iteritems():
            kbDict = {}
            colnames = set([k.upper() for k in kbVal.values()[0][0][1].keys()])
            if colnames.issubset(set([str(c).upper() for c in columns[0]])):
                kbDict[()]=[dict(zip([str(c).upper() for c in columns[0]],
                                    columns[1]))]
            elif colnames.issubset(set(header)):
                try:cols = zip(zip(*[columns[i] for i in [
                            header.index(k.upper()) for k in kbKey]]),rows[1])
                except: cols = ()
                for kIdx, row in cols:
                    if kIdx not in kbDict: kbDict[kIdx] = []
                    kbDict[kIdx].append(dict(zip(header, row)))
            for tk, tables in kbVal.iteritems():
                rDicts = kbDict.get(tuple([str(t).strip('"\' ') for t in tk]),
                                    [{},])
                for table, cols in tables:
                    if table not in results: results[table] = []
                    for rDict in rDicts:
                        result = {}
                        for name, anames in cols.iteritems():
                            res = self.parseValue(rDict.get(name.upper(), None))
                            if type(anames) is not tuple: anames = (anames,)
                            for aname in anames: result[aname] = res
                        if result: results[table].append(result)
        return results


    def query(self, queries):
        resMaps = {}
        for table, query in queries.iteritems():
            resMaps = sortQuery(resMaps, table, query)
        return self.sortedQuery(resMaps)


    def sortedQuery(self, queries):
        def _getQueries(txn, query):
            gostat = re.compile('\bgo\b', re.IGNORECASE)
            try: query = gostat.sub('; ', query)
            except: pass
            query = query.replace('\n', ' ')
            for q in query.split(';'):
                if not q.strip(): continue
                txn.execute(q.strip())
            return txn.description, txn.fetchall()

        def inner(driver):
            try:
                queryResult = {}
                for cs, qs in queries.iteritems():
                    self._dbpool = self.makePool(cs)
                    for query, resMaps in qs.iteritems():
                        if () not in resMaps:
                            if len(resMaps.values()[0].values()) > 1:
                                if query.endswith(' AND '):
                                    query = query[:-5]
                            else:
                                if not query.endswith(' AND '):
                                    query = query + ' WHERE '
                                query = query + ' AND '.join((
                                        ['='.join(k) for k in zip(
                                            resMaps.keys()[0],
                                            resMaps.values()[0].keys()[0])]))
                        log.debug("SQL Query: %s", query)
                        try:
                            yield self._dbpool.runInteraction(_getQueries,query)
                            queryResult.update(self.parseResults(driver.next(),
                                                                    resMaps))
                        except StandardError, ex:
                            queryResult.update(self.parseError(ex, query,
                                                                    resMaps))
                    self.close()
                yield defer.succeed(queryResult)
                driver.next()
            except Exception, ex:
                self.close()
                log.debug("Exception collecting query: %s", str(ex))
                raise
        return drive(inner)


    def run(self):
        def inner(driver):
            try:
                for plugin in self.plugins:
                    pluginName = plugin.name()
                    log.debug("Sending queries for plugin: %s", pluginName)
                    log.debug("Queries: %s" % str(plugin.queries(self.device)))
                    try:
                        yield self.query(plugin.queries(self.device))
                        self.results.append((plugin, driver.next()))
                    except Exception, ex:
                        self.results.append((plugin, ex))
            except Exception, ex:
                raise
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


def SQLGet(cs, query, columns):
    from SQLPlugin import SQLPlugin
    sp = SQLPlugin()
    sp.tables = {'t': (query, {}, cs, columns)}
    cl = SQLClient(device=None, plugins=[sp,])
    cl.run()
    reactor.run()
    for plugin, result in cl.getResults():
        if plugin == sp: return result.get('t', result)
    return result


if __name__ == "__main__":
    cs = "'MySQLdb',host='127.0.0.1',port=3307,db='information_schema',user='zenoss',passwd='zenoss'"
    query = "SHOW GLOBAL STATUS"
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
    if type(results) is not list:
        print results
        sys.exit(1)
    if len(results) > 1 and not isinstance(results[0], Failure):
        print "|".join(results[0].keys())
    for res in results:
        if isinstance(res, Failure):
            print res.getErrorMessage()
        else:
            if len(results) == 1:
                for var, val in res.items():
                    if var in columns.values():
                        var = columns.keys()[columns.values().index(var)]
                    print "%s = %s"%(var, val)
            else: print "|".join([str(r) for r in res.values()])
