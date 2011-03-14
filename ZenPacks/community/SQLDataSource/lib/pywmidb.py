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
__version__ = '1.0.0'

import platform
import datetime
import re
DTPAT = re.compile(r'^(\d{4})-?(\d{2})-?(\d{2})T?(\d{2}):?(\d{2}):?(\d{2})\.?(\d+)?([+|-]\d{2}\d?)?:?(\d{2})?')
WQLPAT = re.compile("^\s*SELECT\s+(?P<props>.+)\s+FROM\s+(?P<cn>\S+)(?:\s+WHERE\s+(?P<kbs>.+))?", re.I)

if platform.system() == 'Windows':
    try:
        import win32com.client
    except:
        raise StandardError, "Can't import win32com.client module. Please, install 'Python Extensions for Windows' first."
    CIM_EMPTY=0
    CIM_SINT8=16
    CIM_UINT8=17
    CIM_SINT16=2
    CIM_UINT16=18
    CIM_SINT32=3
    CIM_UINT32=19
    CIM_SINT64=20
    CIM_UINT64=21
    CIM_REAL32=4
    CIM_REAL64=5
    CIM_BOOLEAN=11
    CIM_STRING=8
    CIM_DATETIME=101
    CIM_REFERENCE=102
    CIM_CHAR16=103
    CIM_OBJECT=13
    CIM_FLAG_ARRAY=0x2000
    CIM_ARR_SINT8=CIM_FLAG_ARRAY|CIM_SINT8
    CIM_ARR_UINT8=CIM_FLAG_ARRAY|CIM_UINT8
    CIM_ARR_SINT16=CIM_FLAG_ARRAY|CIM_SINT16
    CIM_ARR_UINT16=CIM_FLAG_ARRAY|CIM_UINT16
    CIM_ARR_SINT32=CIM_FLAG_ARRAY|CIM_SINT32
    CIM_ARR_UINT32=CIM_FLAG_ARRAY|CIM_UINT32
    CIM_ARR_SINT64=CIM_FLAG_ARRAY|CIM_SINT64
    CIM_ARR_UINT64=CIM_FLAG_ARRAY|CIM_UINT64
    CIM_ARR_REAL32=CIM_FLAG_ARRAY|CIM_REAL32
    CIM_ARR_REAL64=CIM_FLAG_ARRAY|CIM_REAL64
    CIM_ARR_BOOLEAN=CIM_FLAG_ARRAY|CIM_BOOLEAN
    CIM_ARR_STRING=CIM_FLAG_ARRAY|CIM_STRING
    CIM_ARR_DATETIME=CIM_FLAG_ARRAY|CIM_DATETIME
    CIM_ARR_REFERENCE=CIM_FLAG_ARRAY|CIM_REFERENCE
    CIM_ARR_CHAR16=CIM_FLAG_ARRAY|CIM_CHAR16
    CIM_ARR_OBJECT=CIM_FLAG_ARRAY|CIM_OBJECT
    CIM_ILLEGAL=0xfff
    CIM_TYPEMASK=0x2FFF

else:
    try:
        from pysamba.library import *
    except:
        raise StandardError, "Can't import pysamba modules. Please, install pysamba first."
    from pysamba.wbem.wbem import *
    from pysamba.talloc import *
    from pysamba.rpc.credentials import CRED_SPECIFIED

    library.dcom_client_init.restype = c_void_p
    library.dcom_client_init.argtypes = [POINTER(com_context), c_void_p]
    library.com_init_ctx.restype = WERROR
    library.IWbemServices_ExecQuery.restype = WERROR
    library.IEnumWbemClassObject_Reset.restype = WERROR
    library.IUnknown_Release.restype = WERROR

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
    return Date(*datetime.datetime.fromtimestamp(ticks).timetuple()[:3])

def TimeFromTicks(ticks):
    """
    This function constructs an object holding a time value from the given
    ticks value.
    """
    return Time(*datetime.datetime.fromtimestamp(ticks).timetuple()[3:6])

def TimestampFromTicks(ticks):
    """
    This function constructs an object holding a time stamp value from the
    given ticks value.
    """
    return Timestamp(*datetime.datetime.fromtimestamp(ticks).timetuple()[:6])

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
        self.arraysize = 1
        self._rows = []

    @property
    def rowcount(self):
        """
        Returns number of rows affected by last operation. In case
        of SELECTs it returns meaningful information only after
        all rows has been fetched.
        """
        return len(self._rows)

    def _check_executed(self):
        if not self.connection:
            raise InterfaceError, "Connection closed."
        if not self.description:
            raise OperationalError, "No data available. execute() first."

    def __del__(self):
        self.close()

    def close(self):
        """
        Closes the cursor. The cursor is unusable from this point.
        """
        self.description = None
        self.connection = None

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
        if not self.connection:
            raise InterfaceError, "Connection closed."
        self.description = None
        self._rows = []

        # for this method default value for params cannot be None,
        # because None is a valid value for format string.

        if (args != () and len(args) != 1):
            raise TypeError, "execute takes 1 or 2 arguments (%d given)" % (len(args) + 1,)

        if args != ():
            operation = operation%args[0]

        try:
            self.description, self._rows = self.connection._execute(operation)
            if self.description: self.rownumber = 0

        except OperationalError, e:
            raise OperationalError, e
        except InterfaceError, e:
            raise InterfaceError, e

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
        if self.rownumber >= len(self._rows): return None
        result = self._rows[self.rownumber]
        self.rownumber = self.rownumber+1
        return result

    def fetchmany(self, size=None):
        """Fetch up to size rows from the cursor. Result set may be smaller
        than size. If size is not defined, cursor.arraysize is used."""
        self._check_executed()
        end = self.rownumber + (size or self.arraysize)
        result = self._rows[self.rownumber:end]
        self.rownumber = min(end, len(self._rows))
        return result

    def fetchall(self):
        """Fetchs all available rows from the cursor."""
        self._check_executed()
        result = self.rownumber and self._rows[self.rownumber:] or self._rows
        self.rownumber = len(self._rows)
        return result

    def __iter__(self):
        """
        Return self to make cursors compatible with
        Python iteration protocol.
        """
        self._check_executed()
        result = self.rownumber and self._rows[self.rownumber:] or self._rows
        return iter(result)

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
    def __init__(self, user, password, host, namespace):
        self._host = host
        self._ctx = POINTER(com_context)()
        self._pWS = POINTER(IWbemServices)()
        self._wctx = POINTER(IWbemContext)()

        try:
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

            library.com_init_ctx(byref(self._ctx), None)

            creds = user + '%' + password
            cred = library.cli_credentials_init(self._ctx)
            library.cli_credentials_set_conf(cred)
            library.cli_credentials_parse_string(cred, creds, CRED_SPECIFIED)
            library.dcom_client_init(self._ctx, cred)
#            library.lp_do_parameter(-1, "client ntlmv2 auth", 'yes')

            flags = uint32_t()
            flags.value = 0
            result = library.WBEM_ConnectServer(
                        self._ctx,          # com_ctx
                        host,               # server
                        namespace,          # namespace
                        None,               # user
                        None,               # password
                        None,               # locale
                        flags.value,        # flags
                        None,               # authority 
                        self._wctx,         # wbem_ctx 
                        byref(self._pWS))   # services 
            WERR_CHECK(result, self._host, "Connect")

        except WError, e:
            talloc_free(self._ctx)
            raise InterfaceError, e
        except Exception, e:
            talloc_free(self._ctx)
            raise InterfaceError, e

    def _convertArray(self, arr):
        if not arr: return None
        return [arr.contents.item[i] for i in range(arr.contents.count)]

    def _convert(self, v, typeval):
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
            if tt[7] < 30:
                if tt[7] < 0:
                    td = datetime.timedelta(0, tt[7] * 3600 - tt[8] * 60,0)
                else:
                    td = datetime.timedelta(0, tt[7] * 3600 + tt[8] * 60,0)
            else:
                td = datetime.timedelta(0, tt[7] * 60, 0)
            return datetime.datetime(*tt[:7]) - td
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

    def _execute(self, query):
        """
        Execute WQL query and fetch first row
        """
        chunkSize = 10
        try:
            pEnum = POINTER(IEnumWbemClassObject)()
            props, classname, kbs = WQLPAT.match(query).groups('')
            result = library.IWbemServices_ExecQuery(
                        self._pWS,
                        self._ctx,
                        "WQL",
                        query,
                        WBEM_FLAG_RETURN_IMMEDIATELY |
                        WBEM_FLAG_ENSURE_LOCATABLE,
                        None,
                        byref(pEnum))
            WERR_CHECK(result, self._host, "ExecQuery")
            result = library.IEnumWbemClassObject_Reset(pEnum, self._ctx)
            WERR_CHECK(result, self._host, "Reset result of WMI query.");
            assert pEnum
            count = uint32_t()
            objs = (POINTER(WbemClassObject)*chunkSize)()
            library.talloc_increase_ref_count(self._ctx)
            result = library.IEnumWbemClassObject_SmartNext(
                        pEnum,
                        self._ctx,
                        -1,
                        chunkSize,
                        objs,
                        byref(count))
            WERR_CHECK(result, self._host, "Retrieve result data.")
            if count.value == 0: return None, []
            klass = objs[0].contents.obj_class.contents
            inst = objs[0].contents.instance.contents
            typedict = {}
            maxlen = {}
            cimkey = {}
            for j in range(getattr(klass, '__PROPERTY_COUNT')):
                prop = klass.properties[j]
                if not prop.name: continue
                typedict[prop.name] = int(prop.desc.contents.cimtype)
                for i in range(prop.desc.contents.qualifiers.count):
                    q = prop.desc.contents.qualifiers.item[i].contents
                    if q.name in ['key', 'CIM_Key']:
                        if not self._convert(q.value, q.cimtype): continue
                        if prop.desc.contents.cimtype & CIM_TYPEMASK == NUMBER:
                            cimkey[prop.name] = '%s=%%s'%prop.name
                        else:
                            cimkey[prop.name] = '%s="%%s"'%prop.name
                    if q.name == 'MaxLen':
                        maxlen[prop.name] = self._convert(q.value, q.cimtype)
            typedict['__path'] = CIM_STRING
            if '*' in props:
                props = typedict.keys()
            else:
                props = props.replace(' ','').split(',')
                if '__path' not in props: cimkey = {}
            descr = tuple([(pname, typedict.get(pname, CIM_STRING),
                            maxlen.get(pname, None), maxlen.get(pname, None),
                            None, None, None) for pname in props])
            rows = []
            while count.value > 0:
                for i in range(count.value):
                    klass = objs[i].contents.obj_class.contents
                    inst = objs[i].contents.instance.contents
                    pdict = {'_class_name': getattr(klass, '__CLASS', '')}
                    kbs = []
                    for j in range(getattr(klass, '__PROPERTY_COUNT')):
                        prop = klass.properties[j]
                        if not prop.name: continue
                        value = self._convert(inst.data[j],
                                    prop.desc.contents.cimtype & CIM_TYPEMASK)
                        pdict[prop.name] = value
                        if prop.name in cimkey:
                            kbs.append(cimkey[prop.name]%value)
                    pdict['__path'] = pdict['_class_name'] + '.' + ','.join(kbs)
                    rows.append(tuple([pdict.get(p, None) for p in props]))
                assert pEnum
#                count = uint32_t()
#                objs = (POINTER(WbemClassObject)*chunkSize)()
#                library.talloc_increase_ref_count(self._ctx)
                result = library.IEnumWbemClassObject_SmartNext(
                        pEnum,
                        self._ctx,
                        -1,
                        chunkSize,
                        objs,
                        byref(count))
                WERR_CHECK(result, self._host, "Retrieve result data.")
            talloc_free(self._ctx)
            return descr, rows

        except WError, e:
            talloc_free(self._ctx)
            raise OperationalError, e
        except Exception, e:
            talloc_free(self._ctx)
            raise OperationalError, e


    def __del__(self):
        self.close()

    def close(self):
        """
        Close connection to the WMI CIMOM. Implicitly rolls back
        """
        if self._ctx:
            talloc_free(self._ctx)
        self._ctx = None

    def commit(self):
        """
        Commit transaction which is currently in progress.
        """
        if self._cnx:
            return
        else:
            raise InterfaceError, "Connection is closed."
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

### connection object

class win32comCnx:
    """
    This class represent an WMI Connection connection.
    """
    def __init__(self, user, password, host, namespace):
        self._host = host
        self._cnx = None
        try:
            swl = win32com.client.Dispatch("WbemScripting.SWbemLocator")
            self._cnx = swl.ConnectServer(host,namespace,user,password)
        except Exception, e:
            raise InterfaceError, e

    def _convert(self, value, typeval):
        if not value: return None
        if typeval == CIM_DATETIME:
            r = DTPAT.match(value)
            if not r: return str(value)
            tt = map(int, r.groups(0))
            if tt[7] < 30:
                if tt[7] < 0:
                    td = datetime.timedelta(0, tt[7] * 3600 - tt[8] * 60,0)
                else:
                    td = datetime.timedelta(0, tt[7] * 3600 + tt[8] * 60,0)
            else:
                td = datetime.timedelta(0, tt[7] * 60, 0)
            return datetime.datetime(*tt[:7]) - td
        return value

    def _execute(self, query):
        """
        Execute WQL query and fetch first row
        """
        try:
            props, classname, kbs = WQLPAT.match(query).groups('')
            objSet = self._cnx.ExecQuery(query)
            typedict = {}
            maxlen = {}
            for prop in objSet[0].Properties_:
                typedict[prop.Name] = prop.CIMTYPE
            if '*' in props:
                props = typedict.keys()
                props.append('__path')
            else:
                props = props.replace(' ','').split(',')
            descr = tuple([(pname, typedict.get(pname, CIM_STRING),
                            maxlen.get(pname, None), maxlen.get(pname, None),
                            None, None, None) for pname in props])
            rows = []
            for ob in objSet:
                pdict = dict([(p.Name, self._convert(p.Value, p.CIMTYPE)) \
                                                    for p in ob.Properties_])
                pdict['__path'] = ob.Path_.Path.split(':')[1]
                rows.append(tuple([pdict.get(pname, None) for pname in props]))
            return descr, rows

        except Exception, e:
            raise OperationalError, e


    def __del__(self):
        self.close()

    def close(self):
        """
        Close connection to the WMI CIMOM. Implicitly rolls back
        """
        self._cnx = None

    def commit(self):
        """
        Commit transaction which is currently in progress.
        """
        if self._cnx:
            return
        else:
            raise InterfaceError, "Connection is closed."
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
def Connect(user=None, password=None, host=None, namespace=None):

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

    if platform.system() == 'Windows':
        return win32comCnx(user, password, host, namespace)
    else:
        return pysambaCnx(user, password, host, namespace)

connect = Connection = Connect

__all__ = [ 'BINARY', 'Binary', 'Connect', 'Connection', 'DATE',
    'Date', 'Time', 'Timestamp', 'DateFromTicks', 'TimeFromTicks',
    'TimestampFromTicks', 'DataError', 'DatabaseError', 'Error',
    'FIELD_TYPE', 'IntegrityError', 'InterfaceError', 'InternalError',
    'MySQLError', 'NULL', 'NUMBER', 'NotSupportedError', 'DBAPITypeObject',
    'OperationalError', 'ProgrammingError', 'ROWID', 'STRING', 'TIME',
    'TIMESTAMP', 'Warning', 'apilevel', 'connect', 'paramstyle','threadsafety']
