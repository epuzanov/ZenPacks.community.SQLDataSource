################################################################################
#
# This program is part of the SQLDataSource Zenpack for Zenoss.
# Copyright (C) 2012 Egor Puzanov.
#
# This program can be used under the GNU General Public License version 2
# You can find full information here: http://www.zenoss.com/oss
#
################################################################################

__doc__="""pysambaClient

Gets performance data over WMI.

$Id: pysambaClient.py,v1.0 2012/08/06 20:27:28 egor Exp $"""

__version__ = "$Revision: 1.0 $"[11:-2]

from pysamba.library import *
from pysamba.wbem.wbem import *
from pysamba.wbem.Query import Query, QueryResult, _WbemObject, convert
from pysamba.twisted.callback import Callback, WMIFailure
from pysamba.twisted.reactor import eventContext

from twisted.internet import defer

import logging
log = logging.getLogger("zen.pysambaClient")

from datetime import datetime
import re
DTPAT = re.compile(r'^(\d{4})-?(\d{2})-?(\d{2})T?(\d{2}):?(\d{2}):?(\d{2})\.?(\d+)?([+|-]\d{2}\d?)?:?(\d{2})?')

WbemQualifier._fields_ = [
        ('name', CIMSTRING),
        ('flavors', uint8_t),
        ('cimtype', uint32_t),
        ('value', CIMVAR),
        ]

def wbemInstanceWithQualifiersToPython(obj):
    klass = obj.contents.obj_class.contents
    inst = obj.contents.instance.contents
    result = _WbemObject()
    kb = []
    result.__class = klass.__CLASS
    result.__namespace = obj.contents.__NAMESPACE.replace("\\", '/')
    for j in range(klass.__PROPERTY_COUNT):
        prop = klass.properties[j]
        value = convert(inst.data[j], prop.desc.contents.cimtype & CIM_TYPEMASK)
        for qi in range(prop.desc.contents.qualifiers.count):
            q = prop.desc.contents.qualifiers.item[qi].contents
            if q.name in ['key'] and convert(q.value, q.cimtype) == True:
                kb.append("%s=%s"%(prop.name,
                            type(value) is str and '"%s"'%value or value))
        if prop.name:
            setattr(result, prop.name.lower(), value)
    result.__path = "%s.%s"%(klass.__CLASS, ",".join(kb))
    return result

def fetchSome(obj, timeoutMs=-1, chunkSize=10):

    assert obj.pEnum
    count = uint32_t()
    objs = (POINTER(WbemClassObject)*chunkSize)()

    ctx = library.IEnumWbemClassObject_SmartNext_send(
        obj.pEnum, None, timeoutMs, chunkSize)

    def parse(results):
        result = library.IEnumWbemClassObject_SmartNext_recv(
            ctx, obj.ctx, objs, byref(count))

        WERR_CHECK(result, obj._deviceId, "Retrieve result data.")

        result = []
        for i in range(count.value):
            result.append(wbemInstanceWithQualifiersToPython(objs[i]))
            library.talloc_free(objs[i])
        return result

    cback = Callback()
    ctx.contents.async.fn = cback.callback
    d = cback.deferred
    d.addCallback(parse)
    return d

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

class pysambaClient(object):

    def __init__(self, cs):
        """
        @type cs: string
        @param cs: connection string
        """
        self.cs = cs
        self._wmi = None

    def connect(self):
        args, kwargs = parseConnectionString(self.cs)
        lkwargs = dict((k.lower(), v) for k,v in kwargs.iteritems())
        host = lkwargs.get("host") or ""
        user = lkwargs.get("user") or ""
        namespace = lkwargs.get("namespace") or "root\\cimv2"
        log.debug("connect to %s, user %r", host, user)
        if not user:
            log.warning("Windows login name is unset: "
                        "please specify zWinUser and "
                        "zWinPassword zProperties before adding devices.")
            return defer.fail("Username is empty")
        self._wmi = Query()
        creds = '%s%%%s' % (user, lkwargs.get("password", ""))
        return self._wmi.connect(eventContext, host, host, creds, namespace)

    def close(self):
        if self._wmi:
            self._wmi.close()

    def _fetchResults(self, results, timeout, qualifier, result=None, rows=[]):
        if not results:
            return defer.succeed(rows)
        elif isinstance(results, QueryResult):
            result = results
        else:
            for inst in results:
                row = {}
                for aname, value in inst.__dict__.iteritems():
                    if callable(value): continue 
                    if type(value) is str:
                        r = DTPAT.match(str(value))
                        if r:
                            tt = map(int, r.groups(0))
                            if abs(tt[7]) > 30: mins = tt[7]
                            elif tt[7] < 0: mins = 60 * tt[7] - tt[8]
                            else: mins = 60 * tt[7] + tt[8]
                            try: value=datetime(*tt[:7])-timedelta(minutes=mins)
                            except Exception: pass
                    row[aname] = value or ""
                rows.append(row)
        if qualifier:
            d = fetchSome(result, timeoutMs=timeout)
        else:
            d = result.fetchSome(timeoutMs=timeout)
        d.addCallback(self._fetchResults, timeout, qualifier, result, rows)
        return d

    def query(self, task):
        timeout = task.timeout * 1000
        qualifier = task.ds == ''
        d = self._wmi.query(task.sqlp)
        d.addCallback(self._fetchResults, timeout, qualifier)
        return d
