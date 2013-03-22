#***************************************************************************
# pywmidb - A DB API v2.0 compatible interface to WMI.
# Copyright (C) 2011-2013 Egor Puzanov.
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
__version__ = '1.6.1'

from datetime import datetime, timedelta
import threading
LOCK = threading.Lock()
def Lock():
    global LOCK
    return LOCK
import re
DTPAT = re.compile(r'^(\d{4})-?(\d{2})-?(\d{2})T?(\d{2}):?(\d{2}):?(\d{2})\.?(\d+)?([+|-]\d{2}\d?)?:?(\d{2})?')
WQLPAT = re.compile("^\s*SELECT\s+(?P<props>.+)\s+FROM\s+(?P<cn>\S+)(?:\s+WHERE\s+(?P<kbs>.+))?", re.I)
ANDPAT = re.compile("\s+AND\s+", re.I)

WBEM_S_TIMEDOUT = 0x40004L

WERR_BADFUNC = 1

try:
    from pysamba.library import *
except:
    raise StandardError("Can't import pysamba modules. Please, install pysamba first.")
from pysamba.wbem.wbem import *
from pysamba.version import VERSION as PSVERSION

import logging
log = logging.getLogger("zen.pywmidb")

from distutils.version import StrictVersion
if not getattr(WbemQualifier, "_fields_", None):
    if StrictVersion(PSVERSION) < '1.3.10':
        library.WBEM_ConnectServer.restype = WERROR
        library.IEnumWbemClassObject_SmartNext.restype = WERROR
        class IEnumWbemClassObject(Structure): pass
        class IWbemClassObject(Structure): pass
        class IWbemContext(Structure): pass

    library.dcom_client_init.restype = c_void_p
    library.dcom_client_init.argtypes = [POINTER(com_context), c_void_p]
    library.com_init_ctx.restype = WERROR
    library.IWbemServices_ExecQuery.restype = WERROR
    #library.IEnumWbemClassObject_Reset.restype = WERROR
    library.IUnknown_Release.restype = WERROR
    library.dcom_proxy_IUnknown_init.restype = WERROR
    library.dcom_proxy_IWbemLevel1Login_init.restype = WERROR
    library.dcom_proxy_IWbemServices_init.restype = WERROR
    library.dcom_proxy_IEnumWbemClassObject_init.restype = WERROR
    library.dcom_proxy_IRemUnknown_init.restype = WERROR
    library.dcom_proxy_IWbemFetchSmartEnum_init.restype = WERROR
    library.dcom_proxy_IWbemWCOSmartEnum_init.restype = WERROR

    WbemQualifier._fields_ = [
        ('name', CIMSTRING),
        ('flavors', uint8_t),
        ('cimtype', uint32_t),
        ('value', CIMVAR),
        ]

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

def _datetime(v):
    """
    Convert string to datetime.
    """
    r = DTPAT.match(str(v.v_string))
    if not r: return v.v_string
    tt = map(int, r.groups(0))
    if abs(tt[7]) > 30: minutes = tt[7]
    elif tt[7] < 0: minutes = 60 * tt[7] - tt[8]
    else: minutes = 60 * tt[7] + tt[8]
    return datetime(*tt[:7]) - timedelta(minutes=minutes)

def _convertArray(arr):
    """
    Convert array value from CIMTYPE to python types.
    """
    if not arr: return None
    return [arr.contents.item[i] for i in range(arr.contents.count)]

TYPEFUNCT = {CIM_SINT8:lambda v:v.v_sint8, CIM_UINT8:lambda v:v.v_uint8,
    CIM_SINT16:lambda v:v.v_sint16, CIM_UINT16:lambda v:v.v_uint16,
    CIM_SINT32:lambda v:v.v_sint32, CIM_UINT32:lambda v:v.v_uint32,
    CIM_SINT64:lambda v:v.v_sint64, CIM_UINT64:lambda v:v.v_uint64,
    CIM_REAL32:lambda v:float(v.v_uint32),CIM_REAL64:lambda v:float(v.v_uint64),
    CIM_OBJECT:lambda v:v.v_string, CIM_STRING:lambda v:v.v_string,
    CIM_CHAR16:lambda v:v.v_string.decode('utf16'), CIM_DATETIME: _datetime,
    CIM_BOOLEAN:lambda v: str(v).lower() == 'true',
    CIM_REFERENCE:lambda v:v.v_string.startswith(r'\\') and v.v_string.split(
        ':', 1)[-1] or v.v_string,
    CIM_ARR_SINT8:lambda v:_convertArray(v.a_sint8),
    CIM_ARR_UINT8:lambda v:_convertArray(v.a_uint8),
    CIM_ARR_SINT16:lambda v:_convertArray(v.a_sint16),
    CIM_ARR_UINT16:lambda v:_convertArray(v.a_uint16),
    CIM_ARR_SINT32:lambda v:_convertArray(v.a_sint32),
    CIM_ARR_UINT32:lambda v:_convertArray(v.a_uint32),
    CIM_ARR_SINT64:lambda v:_convertArray(v.a_sint64),
    CIM_ARR_UINT64:lambda v:_convertArray(v.a_uint64),
    CIM_ARR_REAL32:lambda v:_convertArray(v.a_real32),
    CIM_ARR_REAL64:lambda v:_convertArray(v.a_real64),
    CIM_ARR_BOOLEAN:lambda v:_convertArray(v.a_boolean),
    CIM_ARR_STRING:lambda v:_convertArray(v.a_string),
    CIM_ARR_DATETIME:lambda v:_convertArray(v.a_datetime),
    CIM_ARR_REFERENCE:lambda v:_convertArray(v.a_reference),
    }

### module constants

# compliant with DB SIG 2.0
apilevel = '2.0'

# module and connection may be shared
threadsafety = 2

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
        self._connection = connection
        self.description = None
        self.rownumber = -1
        self.arraysize = 1
        self._rows = []

    def _check_executed(self):
        if not self._connection:
            raise ProgrammingError("Cursor closed.")
        if not self._connection._creds:
            raise ProgrammingError("Connection closed.")
        if not self.description:
            raise OperationalError("No data available. execute() first.")

    def __del__(self):
        if self._connection:
            self.close()

    def close(self):
        """
        Closes the cursor. The cursor is unusable from this point.
        """
        del self._rows[:]
        self.description = None
        self._connection = None

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
        if not self._connection:
            raise ProgrammingError("Cursor closed.")
        if not self._connection._creds:
            raise ProgrammingError("Connection closed.")
        del self._rows[:]
        self.rownumber = -1
        self.description = None
        good_sql = False

        # for this method default value for params cannot be None,
        # because None is a valid value for format string.

        if (args != () and len(args) != 1):
            raise TypeError("execute takes 1 or 2 arguments (%d given)"%(
                                                                len(args) + 1,))

        if args != ():
            operation = operation%args[0]
        operation = operation.encode('unicode-escape')
        if operation.upper() == 'SELECT 1':
            operation = 'SELECT * FROM __Namespace'
            good_sql = True

        try:
            self.description,self._rows = self._connection._execQuery(operation)
            if good_sql:
                del self._rows[:]
                self._rows.append((1L,))
                self.description = (('1',CIM_UINT64,None,None,None,None,None),)
            if self.description:
                self.rownumber = 0

        except InterfaceError, e:
            raise InterfaceError(e)
        except OperationalError, e:
            raise OperationalError(e)
        except Exception, e:
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
        self._check_executed()
        if not self._rows: return None
        self.rownumber += 1
        return self._rows.pop(0)

    def fetchmany(self, size=None):
        """Fetch up to size rows from the cursor. Result set may be smaller
        than size. If size is not defined, cursor.arraysize is used."""
        self._check_executed()
        if size: size += self.rownumber
        else: size = self.arraysize + self.rownumber
        results = []
        while self._rows and self.rownumber < size:
            self.rownumber += 1
            results.append(self._rows.pop(0))
        return results

    def fetchall(self):
        """Fetchs all available rows from the cursor."""
        self._check_executed()
        results = []
        while self._rows:
            self.rownumber += 1
            results.append(self._rows.pop(0))
        return results

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
        return

    def setoutputsize(self, size=None, column=0):
        """
        This method does nothing, as permitted by DB-API specification.
        """
        return


### connection object

class pysambaCnx:
    """
    This class represent an WMI Connection connection.
    """

    def __init__(self, *args, **kwargs):
        self._timeout = float(kwargs.get('timeout', 30))
        if self._timeout > 0: self._timeout = int(self._timeout * 1000)
        self._host = kwargs.get('host', 'localhost')
        self._ctx = None
        self._pWS = None
        self._wmibatchSize = int(kwargs.get('wmibatchSize', 5))
        self._locale = kwargs.get('locale', None)
        self._namespace = kwargs.get('namespace', 'root/cimv2')
        self._creds = '%s%%%s'%(kwargs.get('user',''),kwargs.get('password',''))
        library.lp_do_parameter(-1, "client ntlmv2 auth",
            kwargs.get('ntlmv2', 'no').lower() == 'yes' and 'yes' or 'no')
        self._lock = Lock()
        self._connect()

    def _connect(self):
        try:
            self._lock.acquire()
            self._ctx = POINTER(com_context)()
            self._pWS = POINTER(IWbemServices)()
            try:
                library.com_init_ctx(byref(self._ctx), None)
                cred = library.cli_credentials_init(self._ctx)
                library.cli_credentials_set_conf(cred)
                library.cli_credentials_parse_string(cred, self._creds, 5)
                library.dcom_client_init(self._ctx, cred)
                flags = uint32_t()
                flags.value = 0
                result = library.WBEM_ConnectServer(
                            self._ctx,                             # com_ctx
                            self._host,                            # server
                            self._namespace,                       # namespace
                            None,                                  # user
                            None,                                  # password
                            self._locale,                          # locale
                            flags.value,                           # flags
                            None,                                  # authority 
                            None,                                  # wbem_ctx
                            byref(self._pWS))                      # services 
                WERR_CHECK(result, self._host, "Connect")
            except Exception, e:
                self.close()
                raise InterfaceError(e)
        finally: self._lock.release()

    def _execQuery(self, operation):
        """
        Executes WQL query
        """
        pEnum = None
        if self._creds and not self._ctx:
            self._connect()
        try:
            self._lock.acquire()
            dDict = {}
            kbs = {}
            rows = []
            result = None
            pEnum = POINTER(IEnumWbemClassObject)()
            ocount = uint32_t()
            ocount.value = self._wmibatchSize
            props, classname, where = WQLPAT.match(operation).groups('')
            if where:
                try:
                    kbs.update(eval('(lambda **kws:kws)(%s)'%ANDPAT.sub(
                                                                    ',',where)))
                    if [v for v in kbs.values() if type(v) is list]:
                        if props == '*': kbkeys = ''
                        else: kbkeys = ',%s'%','.join(kbs.keys())
                        operation='SELECT %s%s FROM %s'%(props,kbkeys,classname)
                    else: kbs.clear()
                except: kbs.clear()
            props = props.upper().replace(' ','').split(',')
            if '*' in props: props.remove('*')
            log.debug('send query: %s', operation)
            result = library.IWbemServices_ExecQuery(
                            self._pWS,
                            self._ctx,
                            "WQL",
                            operation,
                            WBEM_FLAG_FORWARD_ONLY | \
                            WBEM_FLAG_RETURN_IMMEDIATELY | \
                            WBEM_FLAG_ENSURE_LOCATABLE,
                            None,
                            byref(pEnum))
            WERR_CHECK(result, self._host, "ExecQuery")
            log.debug('received enumerator: %s', pEnum)
            while ocount.value == self._wmibatchSize:
                ocount = uint32_t()
                objs = (POINTER(WbemClassObject) * self._wmibatchSize)()
                log.debug('send SmartNext for enumerator: %s', pEnum)
                result = library.IEnumWbemClassObject_SmartNext(
                            pEnum,
                            self._ctx,
                            self._timeout,
                            self._wmibatchSize,
                            objs,
                            byref(ocount))
                WERR_CHECK(result, self._host, "Retrieve result data.")
                log.debug('retrive result from enumerator: %s', pEnum)
                for i in range(ocount.value):
                    try:
                        klass = objs[i].contents.obj_class.contents
                        inst = objs[i].contents.instance.contents
                        pdict = {'__CLASS':getattr(klass, '__CLASS', ''),
                                '__NAMESPACE':getattr(objs[i].contents,
                                '__NAMESPACE','').replace('\\','/'),
                                '__PATH':'%s.'%getattr(klass, '__CLASS', '')}
                        for j in range(getattr(klass, '__PROPERTY_COUNT')):
                            prop = klass.properties[j]
                            if not prop.name: continue
                            uName = prop.name.upper()
                            pType = prop.desc.contents.cimtype & CIM_TYPEMASK
                            pVal = TYPEFUNCT.get(pType,
                                lambda v:v.v_string)(inst.data[j])
                            pdict[uName] = pVal
                            if kbs.get(prop.name, pVal) != pVal:
                                pdict.clear()
                                break
                            if props and '__PATH' not in props and uName in dDict:
                                continue
                            maxlen = None
                            for k in range(prop.desc.contents.qualifiers.count):
                                q=prop.desc.contents.qualifiers.item[k].contents
                                if q.name == 'MaxLen' and not rows:
                                    maxlen = TYPEFUNCT.get(q.cimtype,
                                        lambda v:v.v_string)(q.value)
                                if q.name in ['key']:
                                    if pType != NUMBER:
                                        pVal = '"%s"'%pVal
                                    pdict['__PATH'] += '%s=%s,'%(prop.name,pVal)
                            if uName not in dDict:
                                dDict[uName] = (prop.name, pType,
                                    maxlen, maxlen, None, None, None)
                        if pdict:
                            pdict['__PATH'] = pdict['__PATH'][:-1]
                            rows.append(pdict)
                    finally:
                        library.talloc_free(objs[i])
            if not props and dDict:
                props = dDict.keys() + ['__PATH', '__CLASS', '__NAMESPACE']
            description = tuple([dDict.get(p,(p, 8, None, None, None,
                                    None, None)) for p in props]) or None
            result = [tuple([pd.get(p, None) for p in props]) for pd in rows]
            return description, result
        finally:
            if pEnum:
                result = library.IUnknown_Release(pEnum, self._ctx)
                pEnum = None
            self._lock.release()

    def __del__(self):
        if self._ctx:
            self.close()

    def close(self):
        """
        Close connection to the WMI CIMOM. Implicitly rolls back
        """
        self._creds = None
        self._pWS = None
        if self._ctx:
            log.debug('clean context: %s', self._ctx)
            _ctx, self._ctx = self._ctx, None
            library.talloc_free(_ctx)
        log.debug('connection: %s - closed', self)

    def commit(self):
        """
        Commit transaction which is currently in progress.
        """
        if not self._ctx:
            raise ProgrammingError("Connection closed.")

    def rollback(self):
        """
        Roll back transaction which is currently in progress.
        """
        log.debug('rollback connection: %s', self)
        self._pWS = None
        if self._ctx:
            log.debug('clean context: %s', self._ctx)
            _ctx, self._ctx = self._ctx, None
            library.talloc_free(_ctx)

    def cursor(self):
        """
        Return cursor object that can be used to make queries and fetch
        results from the database.
        """
        if not self._ctx:
            raise ProgrammingError("Connection closed.")
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
    timeout       query timeout in seconds

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
