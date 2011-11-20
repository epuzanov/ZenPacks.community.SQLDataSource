#***************************************************************************
# pywmidb - A DB API v2.0 compatible interface to WMI.
# Copyright (C) 2011 Egor Puzanov.
#
#***************************************************************************
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
# MA  02110-1301  USA
#***************************************************************************

__author__ = "Egor Puzanov"
__version__ = '1.4.4'

from datetime import datetime, timedelta
from distutils.version import StrictVersion
import re
DTPAT = re.compile(r'^(\d{4})-?(\d{2})-?(\d{2})T?(\d{2}):?(\d{2}):?(\d{2})\.?(\d+)?([+|-]\d{2}\d?)?:?(\d{2})?')
WQLPAT = re.compile("^\s*SELECT\s+(?P<props>.+)\s+FROM\s+(?P<cn>\S+)(?:\s+WHERE\s+(?P<kbs>.+))?", re.I)
ANDPAT = re.compile("\s+AND\s+", re.I)

try:
    from pysamba.library import *
except:
    raise StandardError("Can't import pysamba modules. Please, install pysamba first.")
from pysamba.wbem.wbem import *
from pysamba.talloc import *
from pysamba.rpc.credentials import CRED_SPECIFIED
from pysamba.version import VERSION as PSVERSION

library.dcom_client_init.restype = c_void_p
library.dcom_client_init.argtypes = [POINTER(com_context), c_void_p]
library.com_init_ctx.restype = WERROR
library.IWbemServices_ExecQuery.restype = WERROR
library.IEnumWbemClassObject_Reset.restype = WERROR
library.IUnknown_Release.restype = WERROR

if StrictVersion(PSVERSION) < '1.3.10':
    library.WBEM_ConnectServer.restype = WERROR
    library.IEnumWbemClassObject_SmartNext.restype = WERROR
    class IEnumWbemClassObject(Structure): pass
    class IWbemClassObject(Structure): pass
    class IWbemContext(Structure): pass

WbemQualifier._fields_ = [
    ('name', CIMSTRING),
    ('flavors', uint8_t),
    ('cimtype', uint32_t),
    ('value', CIMVAR),
    ]

WBEM_S_TIMEDOUT = 0x40004L

WERR_BADFUNC = 1

class DBAPITypeObject:
    def __init__(self,*values):
        self.values = values
    def __cmp__(self,other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1

def Date(*args):
    """
    This function constructs an object holding a date value.
    """
    return "%04d%02d%02d000000.000000+000"%args

def Time(*args):
    """
    This function constructs an object holding a time value.
    """
    return "00000000%02d%02d%02d.000000:000"%args

def Timestamp(*args):
    """
    This function constructs an object holding a time stamp value.
    """
    return "%04d%02d%02d%02d%02d%02d.000000+000"%args

def DateFromTicks(ticks):
    """
    This function constructs an object holding a date value from the given
    ticks value.
    """
    return Date(*datetime.fromtimestamp(ticks).timetuple()[:3])

def TimeFromTicks(ticks):
    """
    This function constructs an object holding a time value from the given
    ticks value.
    """
    return Time(*datetime.fromtimestamp(ticks).timetuple()[3:6])

def TimestampFromTicks(ticks):
    """
    This function constructs an object holding a time stamp value from the
    given ticks value.
    """
    return Timestamp(*datetime.fromtimestamp(ticks).timetuple()[:6])

def Binary(string):
    """
    This function constructs an object capable of holding a binary (long)
    string value.
    """
    from array import array
    return array('c', x)

STRING = DBAPITypeObject(CIM_STRING, CIM_REFERENCE, CIM_CHAR16, CIM_OBJECT,
                        CIM_BOOLEAN)
BINARY = DBAPITypeObject(CIM_ARR_SINT8, CIM_ARR_UINT8, CIM_ARR_SINT16,
                        CIM_ARR_UINT16, CIM_ARR_SINT32, CIM_ARR_UINT32,
                        CIM_ARR_SINT64, CIM_ARR_UINT64, CIM_ARR_REAL32,
                        CIM_ARR_REAL64, CIM_ARR_BOOLEAN, CIM_ARR_STRING,
                        CIM_ARR_DATETIME, CIM_ARR_REFERENCE, CIM_ARR_CHAR16,
                        CIM_ARR_OBJECT)
NUMBER = DBAPITypeObject(CIM_SINT8, CIM_UINT8, CIM_SINT16, CIM_UINT16,
                        CIM_SINT32, CIM_UINT32, CIM_SINT64, CIM_UINT64,
                        CIM_REAL32, CIM_REAL64)
DATETIME = DBAPITypeObject(CIM_DATETIME)
ROWID = DBAPITypeObject()

### module constants

# compliant with DB SIG 2.0
apilevel = '2.0'

# module may be shared, but not connections
threadsafety = 1

# this module use extended python format codes
paramstyle = 'qmark'

### exception hierarchy

class Warning(StandardError):
    pass

class Error(StandardError):
    pass

class InterfaceError(Error):
    pass

class DatabaseError(Error):
    pass

class DataError(DatabaseError):
    pass

class OperationalError(DatabaseError):
    pass

class IntegrityError(DatabaseError):
    pass

class InternalError(DatabaseError):
    pass

class ProgrammingError(DatabaseError):
    pass

class NotSupportedError(DatabaseError):
    pass


### cursor object

class wmiCursor(object):
    """
    This class emulate a database cursor, which is used to issue queries
    and fetch results from a WMI connection.
    """

    def __init__(self, connection):
        """
        Initialize a Cursor object. connection is a wmiCnx object instance.
        """
        self.connection = connection
        self.description = None
        self.rownumber = -1
        self.arraysize = 5
        self._kbs = {}
        self._host = connection._host
        self._pEnum = None
        self._objs = None
        self._curObj = 0
        self._count = uint32_t()

    def _check_executed(self):
        if not getattr(self.connection, '_ctx', None):
            raise InterfaceError("Connection closed.")
        if not self.description:
            raise OperationalError("No data available. execute() first.")

    def __del__(self):
        self.close()

    def _release(self):
        """
        Release Enumerator and reset objects buffer.
        """
        self._kbs.clear()
        self.description = None
        if self._pEnum:
            try: result = library.IUnknown_Release(self._pEnum,
                            self.connection._ctx)
            except: pass
        self._pEnum = None
        if self._objs != None:
            for i in range(self._curObj, self._count.value):
                talloc_free(self._objs[i])
        self._curObj = 0
        self._objs = None

    def close(self):
        """
        Closes the cursor. The cursor is unusable from this point.
        """
        self._release()
        self._count = None

    def _convertArray(self, arr):
        """
        Convert array value from CIMTYPE to python types.
        """
        if not arr: return None
        return [arr.contents.item[i] for i in range(arr.contents.count)]

    def _convert(self, v, typeval):
        """
        Convert value from CIMTYPE to python types.
        """
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
        if typeval in (CIM_STRING,CIM_REFERENCE): return v.v_string
        if typeval == CIM_CHAR16: return v.v_string.decode('utf16')
        if typeval == CIM_OBJECT: return v.v_string
        if typeval == CIM_DATETIME:
            r = DTPAT.match(str(v.v_string))
            if not r: return v.v_string
            tt = map(int, r.groups(0))
            if abs(tt[7]) > 30: minutes = tt[7]
            elif tt[7] < 0: minutes = 60 * tt[7] - tt[8]
            else: minutes = 60 * tt[7] + tt[8]
            return datetime(*tt[:7]) - timedelta(minutes=minutes)
        if typeval == CIM_ARR_SINT8: return self._convertArray(v.a_sint8)
        if typeval == CIM_ARR_UINT8: return self._convertArray(v.a_uint8)
        if typeval == CIM_ARR_SINT16: return self._convertArray(v.a_sint16)
        if typeval == CIM_ARR_UINT16: return self._convertArray(v.a_uint16)
        if typeval == CIM_ARR_SINT32: return self._convertArray(v.a_sint32)
        if typeval == CIM_ARR_UINT32: return self._convertArray(v.a_uint32)
        if typeval == CIM_ARR_SINT64: return self._convertArray(v.a_sint64)
        if typeval == CIM_ARR_UINT64: return self._convertArray(v.a_uint64)
        if typeval == CIM_ARR_REAL32: return self._convertArray(v.a_real32)
        if typeval == CIM_ARR_REAL64: return self._convertArray(v.a_real64)
        if typeval == CIM_ARR_BOOLEAN: return self._convertArray(v.a_boolean)
        if typeval == CIM_ARR_STRING: return self._convertArray(v.a_string)
        if typeval == CIM_ARR_DATETIME:
            return self._convertArray(v.contents.a_datetime)
        if typeval == CIM_ARR_REFERENCE:
            return self._convertArray(v.contents.a_reference)
        return "Unsupported"


    def execute(self, operation, *args):
        """
        Prepare and execute a database operation (query or command).
        Parameters may be provided as sequence or mapping and will be
        bound to variables in the operation. Parameter style for WSManDb
        is %-formatting, as in:
        cur.execute('select * from table where id=%d', id)
        cur.execute('select * from table where strname=%s', name)
        Please consult online documentation for more examples and
        guidelines.
        """
        if not getattr(self.connection, '_ctx', None):
            raise InterfaceError("Connection closed.")
        self._release()
        self.rownumber = -1

        # for this method default value for params cannot be None,
        # because None is a valid value for format string.

        if (args != () and len(args) != 1):
            raise TypeError("execute takes 1 or 2 arguments (%d given)"%(
                                                                len(args) + 1))

        if args != ():
            operation = operation%args[0]

        try:
            self._pEnum = POINTER(IEnumWbemClassObject)()
            self._objs = (POINTER(WbemClassObject) * self.arraysize)()
            props, classname, where = WQLPAT.match(operation.replace('\\','\\\\'
                                            ).replace('\\\\"','\\"')).groups('')
            if where:
                try:
                    self._kbs.update(eval('(lambda **kws:kws)(%s)'%ANDPAT.sub(
                                                                    ',',where)))
                    if [v for v in self._kbs.values() if type(v) is list]:
                        if props == '*': kbkeys = ''
                        else: kbkeys = ',%s'%','.join(self._kbs.keys())
                        operation='SELECT %s%s FROM %s'%(props,kbkeys,classname)
                    else: self._kbs.clear()
                except: self._kbs.clear()
            props = props.upper().replace(' ','').split(',')
            if '*' in props: props.remove('*')
            result = library.IWbemServices_ExecQuery(
                            self.connection._pWS,
                            self.connection._ctx,
                            "WQL",
                            operation,
                            WBEM_FLAG_FORWARD_ONLY | \
                            WBEM_FLAG_RETURN_IMMEDIATELY | \
                            WBEM_FLAG_ENSURE_LOCATABLE,
                            None,
                            byref(self._pEnum))
            WERR_CHECK(result, self._host, "ExecQuery")
            result = library.IEnumWbemClassObject_SmartNext(
                            self._pEnum,
                            self.connection._ctx,
                            -1,
                            self.arraysize,
                            self._objs,
                            byref(self._count))
            WERR_CHECK(result,self._host,"Retrieve result data.")
            if self._count.value == 0: return
            klass = self._objs[0].contents.obj_class.contents
            inst = self._objs[0].contents.instance.contents
            cimclass = getattr(klass, '__CLASS', '')
            cimnamespace = getattr(self._objs[0].contents, '__NAMESPACE',
                                                        '').replace('\\', '/')
            dDict = {}
            maxlen = {}
            kbKeys = []
            for j in range(getattr(klass, '__PROPERTY_COUNT')):
                prop = klass.properties[j]
                if not prop.name: continue
                uName = prop.name.upper()
                for i in range(prop.desc.contents.qualifiers.count):
                    q = prop.desc.contents.qualifiers.item[i].contents
                    if q.name in ['key']:
                        if uName not in kbKeys: kbKeys.append(uName)
                    if q.name == 'MaxLen':
                        maxlen[uName] = self._convert(q.value, q.cimtype)
                dDict[uName] = (prop.name,
                                prop.desc.contents.cimtype & CIM_TYPEMASK,
                                maxlen.get(uName, None),maxlen.get(uName, None),
                                None, None, None)
            if not props:
                props = dDict.keys()
                props.extend(['__PATH', '__CLASS', '__NAMESPACE'])
            self.description = tuple([dDict.get(p,(p, 8, None, None, None,
                                                None, None)) for p in props])
            if self.description: self.rownumber = 0

        except WError, e:
            self.close()
            raise OperationalError(e)
        except Exception, e:
            self.close()
            raise OperationalError(e)


    def executemany(self, operation, param_seq):
        """
        Execute a database operation repeatedly for each element in the
        parameter sequence. Example:
        cur.executemany("INSERT INTO table VALUES(%s)", [ 'aaa', 'bbb' ])
        """
        for params in param_seq:
            self.execute(operation, params)

    def nextset(self):
        """
        This method makes the cursor skip to the next available result set,
        discarding any remaining rows from the current set. Returns true
        value if next result is available, or None if not.
        """
        self._check_executed()
        return None

    def fetchone(self):
        """Fetches a single row from the cursor. None indicates that
        no more rows are available."""
        return (self.fetchmany(size=1) or (None,))[0]

    def fetchmany(self, size=None):
        """Fetch up to size rows from the cursor. Result set may be smaller
        than size. If size is not defined, cursor.arraysize is used."""
        self._check_executed()
        if not size: size = self.arraysize
        results = []
        props = [p[0].upper() for p in self.description]
        try:
            while self._count.value != 0:
                for self._curObj in range(self._curObj, self._count.value):
                    iPath = []
                    klass = self._objs[self._curObj].contents.obj_class.contents
                    inst = self._objs[self._curObj].contents.instance.contents
                    pdict = {'__CLASS':getattr(klass, '__CLASS', ''),
                        '__NAMESPACE':getattr(self._objs[self._curObj].contents,
                                            '__NAMESPACE','').replace('\\','/')}
                    for j in range(getattr(klass, '__PROPERTY_COUNT')):
                        prop = klass.properties[j]
                        if not prop.name: continue
                        uName = prop.name.upper()
                        pType = prop.desc.contents.cimtype & CIM_TYPEMASK
                        pVal = self._convert(inst.data[j], pType)
                        pdict[uName] = pVal
                        if self._kbs.get(prop.name, pVal) != pVal:
                            pdict.clear()
                            break
                        if '__PATH' not in props: continue
                        for i in range(prop.desc.contents.qualifiers.count):
                            q = prop.desc.contents.qualifiers.item[i].contents
                            if q.name not in ['key']: continue
                            if pType == NUMBER:
                                iPath.append('%s=%s'%(prop.name, pVal))
                            else:
                                iPath.append('%s="%s"'%(prop.name, pVal))
                    talloc_free(self._objs[self._curObj]) 
                    if not pdict: continue
                    if '__PATH' in props:
                        pdict['__PATH'] = '%s.%s'%( pdict['__CLASS'],
                                                            ','.join(iPath))
                    results.append(tuple([pdict.get(p, None) for p in props]))
                    pdict.clear()
                    self.rownumber += 1
                    if len(results) == size: break
                self._curObj += 1
                if len(results) == size: break
                self._curObj = 0
                del self._objs
                self._objs = (POINTER(WbemClassObject) * self.arraysize)()
                result = library.IEnumWbemClassObject_SmartNext(
                            self._pEnum,
                            self.connection._ctx,
                            -1,
                            self.arraysize,
                            self._objs,
                            byref(self._count))
                WERR_CHECK(result,self._host,"Retrieve result data.")
            return results

        except WError, e:
            self.close()
            raise OperationalError(e)
        except Exception, e:
            self.close()
            raise OperationalError(e)

    def fetchall(self):
        """Fetchs all available rows from the cursor."""
        return self.fetchmany(size=-1)

    def next(self):
        """Fetches a single row from the cursor. None indicates that
        no more rows are available."""
        row = self.fetchone()
        if not row: raise StopIteration
        return row

    def __iter__(self):
        """
        Return self to make cursors compatible with
        Python iteration protocol.
        """
        self._check_executed()
        return self

    def setinputsizes(self, sizes=None):
        """
        This method does nothing, as permitted by DB-API specification.
        """
        self._check_executed()

    def setoutputsize(self, size=None, column=0):
        """
        This method does nothing, as permitted by DB-API specification.
        """
        self._check_executed()

### connection object

class pysambaCnx:
    """
    This class represent an WMI Connection connection.
    """

    def __init__(self, *args, **kwargs):
        self._host = kwargs.get('host', 'localhost')
        self._ctx = POINTER(com_context)()
        self._pWS = POINTER(IWbemServices)()
        self._wmibatchSize = kwargs.get('wmibatchSize', 5)
        if not library.lp_loaded():
            library.lp_load()
            library.dcerpc_init()
            library.dcerpc_table_init()
            library.dcom_proxy_IUnknown_init()
            library.dcom_proxy_IWbemLevel1Login_init()
            library.dcom_proxy_IWbemServices_init()
            library.dcom_proxy_IEnumWbemClassObject_init()
            library.dcom_proxy_IRemUnknown_init()
            library.dcom_proxy_IWbemFetchSmartEnum_init()
            library.dcom_proxy_IWbemWCOSmartEnum_init()

        try:
            library.com_init_ctx(byref(self._ctx), None)

            creds = '%s%%%s'%(kwargs.get('user', ''), kwargs.get('password', ''))
            cred = library.cli_credentials_init(self._ctx)
            library.cli_credentials_set_conf(cred)
            library.cli_credentials_parse_string(cred, creds, CRED_SPECIFIED)
            library.dcom_client_init(self._ctx, cred)
            library.lp_do_parameter(-1, "client ntlmv2 auth",
                                    kwargs.get('ntlmv2', 'no').lower())

            flags = uint32_t()
            flags.value = 0
            result = library.WBEM_ConnectServer(
                        self._ctx,                             # com_ctx
                        self._host,                            # server
                        kwargs.get('namespace', 'root/cimv2'), # namespace
                        None,                                  # user
                        None,                                  # password
                        kwargs.get('locale', None),            # locale
                        flags.value,                           # flags
                        None,                                  # authority 
                        POINTER(IWbemContext)(),               # wbem_ctx 
                        byref(self._pWS))                      # services 
            WERR_CHECK(result, self._host, "Connect")

        except WError, e:
            self.close()
            raise InterfaceError(e)

        except Exception, e:
            self.close()
            raise InterfaceError(e)

    def __del__(self):
        self.close()

    def close(self):
        """
        Close connection to the WMI CIMOM. Implicitly rolls back
        """
        self._pWS = None
        if self._ctx:
            talloc_free(self._ctx)
        self._ctx = None

    def commit(self):
        """
        Commit transaction which is currently in progress.
        """
        return

    def rollback(self):
        """
        Roll back transaction which is currently in progress.
        """
        return

    def cursor(self):
        """
        Return cursor object that can be used to make queries and fetch
        results from the database.
        """
        return wmiCursor(self)

    def autocommit(self, status):
        """
        Turn autocommit ON or OFF.
        """
        return


# connects to a WMI CIMOM
def Connect(*args, **kwargs):

    """
    Constructor for creating a connection to the WMI. Returns
    a WMI Connection object. Paremeters are as follows:

    user          user to connect as
    password      user's password
    host          host name
    namespace     namespace

    Examples:
    con  =  pywmidb.connect(user='user',
                            password='P@ssw0rd'
                            host='localhost',
                            namespace='root/cimv2',
                            )
    """

    return pysambaCnx(*args, **kwargs)

connect = Connection = Connect

__all__ = [ 'BINARY', 'Binary', 'Connect', 'Connection', 'DATE',
    'Date', 'Time', 'Timestamp', 'DateFromTicks', 'TimeFromTicks',
    'TimestampFromTicks', 'DataError', 'DatabaseError', 'Error',
    'FIELD_TYPE', 'IntegrityError', 'InterfaceError', 'InternalError',
    'NULL', 'NUMBER', 'NotSupportedError', 'DBAPITypeObject',
    'OperationalError', 'ProgrammingError', 'ROWID', 'STRING', 'TIME',
    'TIMESTAMP', 'Warning', 'apilevel', 'connect', 'paramstyle','threadsafety']
