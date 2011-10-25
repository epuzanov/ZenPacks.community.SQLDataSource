################################################################################
#
# This program is part of the SQLDataSource Zenpack for Zenoss.
# Copyright (C) 2011 Egor Puzanov.
#
# This program can be used under the GNU General Public License version 2
# You can find full information here: http://www.zenoss.com/oss
#
################################################################################

__doc__="""WMIQuery

Gets WMI performance data.

$Id: WMIQuery.py,v 1.0 2011/10/25 16:21:40 egor Exp $"""

__version__ = "$Revision: 1.0 $"[11:-2]

from pysamba.twisted.callback import WMIFailure
from pysamba.wbem.Query import Query
from twisted.python.failure import Failure

from Products.ZenUtils.Driver import drive

import logging
log = logging.getLogger("zen.SQLClient")

from DateTime import DateTime
import re
DTPAT=re.compile(r'^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})\.(\d{6})([+|-])(\d{3})')

class BadCredentials(Exception): pass


class wmiQuery(object):

    def __init__(self, sqlp, client):
        self.sql = ''
        self.sqlp = sqlp
        self.resMaps = {}
        self.results = client.results


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
        err = Failure(err)
        err.value = 'Received error (%s) from query: %s'%(err.value, self.sql)
        log.error(err.getErrorMessage())
        for instances in self.resMaps.values():
            for tables in instances.values():
                for (pname, table), props in tables:
                    self.results.setdefault(pname,{})[table] = [err,]


    def parseResult(self, instances):
        for insts in self.resMaps.values():
            for tables in insts.values():
                for (pn, table), props in tables:
                    self.results.setdefault(pn, {})[table] = []
        for instance in instances:
            for kbKey, kbVal in self.resMaps.iteritems():
                cNames=set([k.upper() for k in kbVal.values()[0][0][1].keys()])
                kIdx = []
                for kb in kbKey:
                    kbV = getattr(instance, kb, '')
                    if kbV is list: kbV = ' '.join(kbV)
                    kIdx.append(str(kbV).upper())
                for (pn, table), properties in kbVal.get(tuple(kIdx), []):
                    result = {}
                    if len(properties) == 0:
                        properties = instance.__dict__.keys()
                    if type(properties) is not dict:
                        properties = dict(zip(properties, properties))
                    for name, anames in properties.iteritems():
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
                        if not hasattr(anames, '__iter__'): anames=(anames,)
                        for aname in anames: result[aname] = res
                    if result: self.results.setdefault(pn, {}).setdefault(
                                                        table,[]).append(result)


    def run(self, pool):
        def inner(driver):
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
            except WMIFailure, ex:
                self.parseError(ex)
                msg = 'Received %s from query: %s'

                # Look for specific errors that should be equated
                # to an empty result set.
                if str(ex) in (
                    "NT code 0x80041010",
                    "WBEM_E_INVALID_CLASS",
                    ):
                    log.debug(msg % (ex, query))
                else:
                    log.error(msg % (ex, query))
                    raise
        return drive(inner)


class wmiPool(object):

    def __init__(self, cs):
        self.cs = cs
        self.queries = []
        self._wmi = None
        args, kwargs = self.parseCS(cs)
        self.host = kwargs.get('host', 'localhost')
        self.user = kwargs.get('user', '')
        self.password = kwargs.get('password', '')
        self.namespace = kwargs.get('namespace', 'root/cimv2')


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


    def add(self, pname, tname, task, client):
        sqlp = task[0]
        for query in self.queries:
            if query.sqlp != sqlp: continue
            query.add(pname, tname, task)
            return
        self.queries.append(wmiQuery(task[0], client))
        self.queries[-1].add(pname, tname, task)


    def run(self):
        from pysamba.twisted.reactor import eventContext
        def inner(driver):
            if not self.user:
                log.warning("Windows login name is unset: "
                            "please specify zWinUser and "
                            "zWinPassword zProperties before adding devices.")
                raise BadCredentials("Username is empty")
            log.debug("connect to %s, user %s", self.host, self.user)
            pool = None
            try:
                pool = Query()
                yield pool.connect(eventContext, self.host, self.host,
                            '%s%%%s'%(self.user, self.password), self.namespace)
                driver.next()
            except Exception, ex:
                log.debug("Exception collecting query: %s", str(ex))
                if hasattr(pool, 'close'): pool.close()
                raise
            for query in self.queries:
                try:
                    yield query.run(pool)
                    driver.next()
                except: continue
            pool.close()
        return drive(inner)
