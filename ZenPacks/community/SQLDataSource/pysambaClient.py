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

$Id: pysambaClient.py,v1.1 2012/08/08 19:15:52 egor Exp $"""

__version__ = "$Revision: 1.1 $"[11:-2]

from pysamba.library import *
from pysamba.wbem.wbem import *
from pysamba.wbem.Query import Query, QueryResult
from pysamba.twisted.callback import Callback, WMIFailure
from pysamba.twisted.reactor import eventContext

from twisted.internet import defer

import logging
log = logging.getLogger("zen.pysambaClient")

from datetime import datetime, timedelta
import re
DTPAT = re.compile(r'^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})\.(\d{6})([+|-]\d{3})')
TDPAT = re.compile(r'^(\d{8})(\d{2})(\d{2})(\d{2})\.(\d{6})')

if not getattr(WbemQualifier, "_fields_", None):
    WbemQualifier._fields_ = [
        ('name', CIMSTRING),
        ('flavors', uint8_t),
        ('cimtype', uint32_t),
        ('value', CIMVAR),
        ]

def convertArray(arr):
    if not arr: return None
    return [arr.contents.item[i] for i in range(arr.contents.count)]

def convert(v, typeval):
    if typeval == CIM_SINT8: return v.v_sint8
    if typeval == CIM_UINT8: return v.v_uint8
    if typeval == CIM_SINT16: return v.v_sint16
    if typeval == CIM_UINT16: return v.v_uint16
    if typeval == CIM_SINT32: return v.v_sint32
    if typeval == CIM_UINT32: return v.v_uint32
    if typeval == CIM_SINT64: return v.v_sint64
    if typeval == CIM_UINT64: return v.v_sint64
    if typeval == CIM_REAL32: return float(v.v_uint32)
    if typeval == CIM_REAL64: return float(v.v_uint64)
    if typeval == CIM_BOOLEAN: return bool(v.v_boolean)
    if typeval == CIM_DATETIME:
        dt = str(v.v_datetime)
        s = DTPAT.match(dt)
        if s is not None:
            tt = map(int, s.groups(0))
            return datetime(*tt[:7]) #+ timedelta(minutes=tt[7])
        s = TDPAT.match(dt)
        if s is None: return str(dt)
        try: return timedelta(**dict(zip(
            ('days','hours','minutes','microseconds'),map(int, s.groups(0)))))
        except Exception: return v.v_string or ""
    if typeval == CIM_STRING:
        return v.v_string or ""
    if typeval == CIM_REFERENCE:
        if not v.v_string.startswith(r'\\'): return v.v_string
        return v.v_string.split(':', 1)[-1]
    if typeval == CIM_CHAR16:
        return v.v_string.decode('utf16')
    if typeval == CIM_OBJECT:
        return v.v_string or ""
    if typeval == CIM_ARR_SINT8: return convertArray(v.a_sint8)
    if typeval == CIM_ARR_UINT8: return convertArray(v.a_uint8)
    if typeval == CIM_ARR_SINT16: return convertArray(v.a_sint16)
    if typeval == CIM_ARR_UINT16: return convertArray(v.a_uint16)
    if typeval == CIM_ARR_SINT32: return convertArray(v.a_sint32)
    if typeval == CIM_ARR_UINT32: return convertArray(v.a_uint32)
    if typeval == CIM_ARR_SINT64: return convertArray(v.a_sint64)
    if typeval == CIM_ARR_UINT64: return convertArray(v.a_uint64)
    if typeval == CIM_ARR_REAL32: return convertArray(v.a_real32)
    if typeval == CIM_ARR_REAL64: return convertArray(v.a_real64)
    if typeval == CIM_ARR_BOOLEAN: return convertArray(v.a_boolean)
    if typeval == CIM_ARR_STRING: return convertArray(v.a_string)
    if typeval == CIM_ARR_DATETIME:
        return convertArray(v.contents.a_datetime)
    if typeval == CIM_ARR_REFERENCE:
        return convertArray(v.contents.a_reference)
    return "Unsupported"

def wbemInstanceToPython(obj, qualifiers=False):
    klass = obj.contents.obj_class.contents
    inst = obj.contents.instance.contents
    result = {}
    kb = []
    result['__class'] = klass.__CLASS
    result['__namespace'] = obj.contents.__NAMESPACE.replace("\\", '/')
    for j in range(klass.__PROPERTY_COUNT):
        prop = klass.properties[j]
        value = convert(inst.data[j], prop.desc.contents.cimtype & CIM_TYPEMASK)
        if qualifiers:
            for qi in range(prop.desc.contents.qualifiers.count):
                q = prop.desc.contents.qualifiers.item[qi].contents
                if q.name in ['key'] and convert(q.value, q.cimtype) == True:
                    kb.append("%s=%s"%(prop.name,
                            type(value) is str and '"%s"'%value or value))
        if prop.name:
            result[prop.name.lower()] = value
    if qualifiers:
        result['__path'] = "%s.%s"%(klass.__CLASS, ",".join(kb))
    return result

def fetchSome(obj, timeoutMs=-1, chunkSize=10, qualifiers=False):

    assert obj.pEnum

    ctx = library.IEnumWbemClassObject_SmartNext_send(
        obj.pEnum, None, timeoutMs, chunkSize
        )

    cback = Callback()
    ctx.contents.async.fn = cback.callback

    def fetch(results):
        count = uint32_t()
        objs = (POINTER(WbemClassObject)*chunkSize)()
        result = library.IEnumWbemClassObject_SmartNext_recv(
            ctx, obj.ctx, objs, byref(count)
            )

        WERR_CHECK(result, obj._deviceId, "Retrieve result data.")

        result = []
        for i in range(count.value):
            result.append(wbemInstanceToPython(objs[i], qualifiers))
            library.talloc_free(objs[i])
        return result

    d = cback.deferred
    d.addCallback(fetch)
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
        self.ready = None

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
        self._wmi.pWS = None
        self._wmi = None
        self.ready = None

    def _fetchResults(self, results, timeout, qualifiers, result=None, rows=[]):
        if not results:
            return defer.succeed(rows)
        elif isinstance(results, QueryResult):
            rows = []
            result = results
        else:
            rows.extend(results)
        d = fetchSome(result, timeoutMs=timeout, qualifiers=qualifiers)
        d.addCallback(self._fetchResults, timeout, qualifiers, result, rows)
        return d

    def query(self, task):
        timeout = task.timeout * 1000
        qualifiers = task.ds == ''
        rows = []
        d = self._wmi.query(task.sqlp)
        d.addCallback(self._fetchResults, timeout, qualifiers, rows)
        return d
