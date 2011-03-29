#***************************************************************************
# pywbemdb - A DB API v2.0 compatible interface to WBEM.
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
__version__ = '1.0.3'

try:
    import pywbem
except:
    raise StandardError, "Can't import pywbem module. Please, install pywbem first."

import datetime
import re
WQLPAT = re.compile("^\s*SELECT\s+(?P<props>.+)\s+FROM\s+(?P<cn>\S+)(?:\s+WHERE\s+(?P<kbs>.+))?", re.I)
ANDPAT = re.compile("\s+AND\s+", re.I)

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

class wbemCursor(object):
    """
    This class emulate a database cursor, which is used to issue queries
    and fetch results from a WBEM connection.
    """

    def __init__(self, connection):
        """
        Initialize a Cursor object. connection is a wbemCnx object instance.
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

class pywbemCnx:
    """
    This class represent an WBEM Connection connection.
    """
    def __init__(self, *args, **kwargs):
        self._host = kwargs['host']
        url = '%s://%s:%s'%(kwargs['scheme'], kwargs['host'], kwargs['port'])
        self._cnx = pywbem.WBEMConnection(url,
                                        (kwargs['user'], kwargs['password']),
                                        default_namespace=kwargs['namespace'])

    def _convert(self, value, is_array):
        """
        Convert CIM types to Python standard types.
        """
        if not value: return None
        if is_array:
            return [self._convert(v, None) for v in value]
        if isinstance(value, pywbem.Uint8): return int(value)
        if isinstance(value, pywbem.Uint16): return int(value)
        if isinstance(value, pywbem.Uint32): return int(value)
        if isinstance(value, pywbem.Uint64): return long(value)
        if isinstance(value, pywbem.Sint8): return int(value)
        if isinstance(value, pywbem.Sint16): return int(value)
        if isinstance(value, pywbem.Sint32): return int(value)
        if isinstance(value, pywbem.Sint64): return long(value)
        if isinstance(value, pywbem.Real32): return float(value)
        if isinstance(value, pywbem.Real64): return float(value)
        if isinstance(value, pywbem.CIMDateTime):
            return datetime.datetime(*value.datetime.utctimetuple()[:7])
        return value

    def _parseType(self, ptype, is_array):
        """
        Convert CIM types string to CIMTYPE value.
        """
        return (is_array and CIM_FLAG_ARRAY or 0)|{'sint16':CIM_SINT16,
                'sint32':CIM_SINT32, 'real32':CIM_REAL32, 'real64':CIM_REAL64,
                'string': CIM_STRING, 'boolean':CIM_BOOLEAN,'object':CIM_OBJECT,
                'sint8':CIM_SINT8, 'uint8':CIM_UINT8, 'uint16':CIM_UINT16,
                'uint32':CIM_UINT32, 'sint64':CIM_SINT64, 'uint64':CIM_UINT64,
                'datetime':CIM_DATETIME, 'reference':CIM_REFERENCE,
                'char16':CIM_CHAR16}.get(ptype, 0)

    def _execute(self, operation):
        """
        Execute Query
        """
        try:
            props, classname, kbs = WQLPAT.match(operation).groups('')
        except:
            raise ProgrammingError, "Syntax error in the WQL statement."
        kwargs = {"IncludeQualifiers":True, "LocalOnly":False}
        if props != '*':
            plist = set(props.replace(' ', '').split(','))
            if '__path' in plist: plist.remove('__path')
            kwargs["PropertyList"] = list(plist)
        try:
            if kbs:
                try:
                    kbs=eval('(lambda **kwargs:kwargs)(%s)'%ANDPAT.sub(',',kbs))
                    instancename = pywbem.CIMInstanceName(classname, kbs)
                    results = [self._cnx.GetInstance(instancename, **kwargs)]
                except SyntaxError:
                    results = self._cnx.ExecQuery('WQL', operation)
                    if isinstance(results, pywbem.CIMInstance):
                        results = [results]
                    if not results:
                        raise
            else:
                results = self._cnx.EnumerateInstances(classname,**kwargs)
        except pywbem.CIMError, e:
            raise InterfaceError, e
        except pywbem.cim_http.AuthError:
            raise InterfaceError, "Bad credentials."
        try:
            typedict = {}
            maxlen = {}
            for pname, prop in results[0].properties.iteritems():
                if not prop: continue
                typedict[pname] = self._parseType(prop.type,
                                                    prop.is_array)
                if 'maxlen' in prop.qualifiers:
                    maxlen[pname] = prop.qualifiers['MaxLen'].value
            typedict['__path'] = CIM_STRING
            if '*' in props:
                props = typedict.keys()
            else:
                props = props.replace(' ','').split(',')
            descr = tuple([(pname, typedict.get(pname, CIM_STRING),
                            maxlen.get(pname, None), maxlen.get(pname, None),
                            None, None, None) for pname in props])
            rows = []
            for ob in results:
                pdict = dict([(pname, self._convert(p.value, p.is_array)) \
                                    for pname, p in ob.properties.iteritems()])
                pdict['__path'] = str(ob.path).split(':', 1)[1]
                rows.append(tuple([pdict.get(pname, None) for pname in props]))
            return descr, rows

        except IndexError, e:
            raise OperationalError, "No data available."
        except pywbem.CIMError, e:
            raise OperationalError, e
        except Exception, e:
            raise OperationalError, e

    def __del__(self):
        self.close()

    def close(self):
        """
        Close connection to the WBEM CIMOM. Implicitly rolls back
        """
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
        return wbemCursor(self)

    def autocommit(self, status):
        """
        Turn autocommit ON or OFF.
        """
        return

# connects to a WBEM CIMOM
def Connect(*args, **kwargs):

    """
    Constructor for creating a connection to the WBEM. Returns
    a WBEM Connection object. Paremeters are as follows:

    scheme        http or https
    port          port
    user          user to connect as
    password      user's password
    host          host name
    namespace     namespace

    Examples:
    con  =  pywbemdb.connect(scheme='https',
                            port=5989,
                            user='user',
                            password='P@ssw0rd'
                            host='localhost',
                            namespace='root/cimv2',
                            )
    """

    return pywbemCnx(*args, **kwargs)

connect = Connection = Connect

__all__ = [ 'BINARY', 'Binary', 'Connect', 'Connection', 'DATE',
    'Date', 'Time', 'Timestamp', 'DateFromTicks', 'TimeFromTicks',
    'TimestampFromTicks', 'DataError', 'DatabaseError', 'Error',
    'FIELD_TYPE', 'IntegrityError', 'InterfaceError', 'InternalError',
    'MySQLError', 'NULL', 'NUMBER', 'NotSupportedError', 'DBAPITypeObject',
    'OperationalError', 'ProgrammingError', 'ROWID', 'STRING', 'TIME',
    'TIMESTAMP', 'Warning', 'apilevel', 'connect', 'paramstyle','threadsafety']
