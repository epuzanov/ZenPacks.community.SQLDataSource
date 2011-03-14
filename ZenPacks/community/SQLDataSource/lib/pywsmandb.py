#***************************************************************************
# pywsmandb - A DB API v2.0 compatible interface to WS-Management.
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

from xml.dom.minidom import parseString
from xml.dom.minicompat import NodeList
import datetime
import re
DTPAT = re.compile(r'^(\d{4})-?(\d{2})-?(\d{2})T?(\d{2}):?(\d{2}):?(\d{2})\.?(\d+)?([+|-]\d{2}\d?)?:?(\d{2})?')
WQLPAT = re.compile("^\s*SELECT\s+(?P<props>.+)\s+FROM\s+(?P<cn>\S+)(?:\s+WHERE\s+(?P<kbs>.+))?", re.I)

import platform
if platform.system() == 'Windows':
    try:
        import win32com.client
    except:
        raise StandardError, "Can't import win32com.client module. Please, install 'Python Extensions for Windows' first."
else:
    try:
        import pywsman
    except:
        raise StandardError, "Can't import pywsman module. Please, install pywsman first."

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

STRING = DBAPITypeObject(8)
BINARY = DBAPITypeObject()
NUMBER = DBAPITypeObject(8)
DATETIME = DBAPITypeObject(101)
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

class wsmanCursor(object):
    """
    This class emulate a database cursor, which is used to issue queries
    and fetch results from a WS-Management connection.
    """

    def __init__(self, connection):
        """
        Initialize a Cursor object. connection is a wsmanCnx object instance.
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

class baseCnx:
    """
    This base class represent an WS-Management Connection connection.
    """

    def _parseXml(self, xmlroot):
        """
        parse XML output.
        """
        XML_NS_ADDRESSING="http://schemas.xmlsoap.org/ws/2004/08/addressing"
        XML_NS_WS_MAN="http://schemas.dmtf.org/wbem/wsman/1/wsman.xsd"
        XML_NS_ENUMERATION="http://schemas.xmlsoap.org/ws/2004/09/enumeration"
        dicts = []
        root = parseString(xmlroot)
        if isinstance(root, NodeList): root = root[0]
        root = root.getElementsByTagNameNS(XML_NS_ENUMERATION,'Items') or root
        if isinstance(root, NodeList): root = root[0]
        items = root.getElementsByTagNameNS(XML_NS_WS_MAN,'Item')
        eprs=root.getElementsByTagNameNS(XML_NS_ADDRESSING,'ReferenceParameters')
        for item, epr in (zip(items, eprs) or zip([root], [root])):
            pdict = {}
            kbs = ['%s="%s"'%(s.getAttributeNode('Name').nodeValue,
                              s.firstChild.nodeValue
                    ) for s in epr.getElementsByTagNameNS(XML_NS_WS_MAN,
                                                                'Selector')]
            if kbs:
                pdict['__namespace'],cn=str(epr.firstChild.firstChild.nodeValue
                                                                ).rsplit('/', 1)
                pdict['__path'] = cn + '.' + ','.join(kbs)
            for prop in item.firstChild.childNodes:
                if prop.hasChildNodes:
                    value = prop.firstChild
                    if value: value = value.nodeValue
                else: value = None
                if prop.localName in pdict:
                    if type(pdict[prop.localName]) != list:
                        pdict[prop.localName] = [pdict[prop.localName],]
                    pdict[prop.localName].append(value)
                else:
                    pdict[prop.localName] = value
            dicts.append(pdict)
        return dicts

    def _convert(self, value):
        value = str(value)
        if value.isdigit(): return long(value)
        if value.replace('.', '', 1).isdigit(): return float(value)
        if value == 'false': return False
        if value == 'true': return True
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

    def __del__(self):
        self.close()

    def close(self):
        """
        Close connection to the WS-Management CIMOM. Implicitly rolls back
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
        return wsmanCursor(self)

    def autocommit(self, status):
        """
        Turn autocommit ON or OFF.
        """
        return


class pywsmanCnx(baseCnx):
    """
    This class represent an WS-Management Connection connection.
    """
    def __init__(self, scheme, port, path, user, password, host, namespace):
        self._host = host
        self._namespace = namespace
        self._options = pywsman.ClientOptions()
        self._cnx = pywsman.Client(host, int(port), path, scheme, user,password)
        doc = self._cnx.identify( self._options )
        if not doc:
            raise OperationalError,"Access denied for user '%s' to '%s://%s:%s%s'"%(
                                                user, scheme, host, port, path)
        if 'Microsoft' in str(doc):
            self._dialect = pywsman.WSM_WQL_FILTER_DIALECT
        else:
            self._dialect = pywsman.WSM_XPATH_FILTER_DIALECT

    def _execute(self, query):
        """
        Execute WQL query and fetch first row
        """
        try:
            props, classname, kbs = WQLPAT.match(query).groups('')
            props = props.replace(' ','').split(',')
            if ('*' in props) or ('__path' in props) or ('__namespace' in props):
                self._options.set_flags(pywsman.FLAG_ENUMERATION_ENUM_OBJ_AND_EPR)
            else:
                self._options.clear_flags(pywsman.FLAG_ENUMERATION_ENUM_OBJ_AND_EPR)
            fltr = pywsman.Filter()
            fltr.simple(self._dialect, query)
            if self._dialect == pywsman.WSM_WQL_FILTER_DIALECT:
                if self._namespace.startswith('http'):
                    uri = self._namespace + '/*'
                else:
                    uri="http://schemas.microsoft.com/wbem/wsman/1/wmi/%s/*"%\
                                                                self._namespace
            else:
                if self._namespace.startswith('http'):
                    uri = self._namespace + '/' + classname
                else:
                    uri = "http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/"+\
                                                                    classname
            doc = self._cnx.enumerate( self._options, fltr, uri )
            if doc.is_fault(): raise OperationalError, doc.fault().reason()
            root = doc.root()
            ctx = root.find(pywsman.XML_NS_ENUMERATION, "EnumerationContext")
            rows = []
            descr = None
            while ctx:
                doc = self._cnx.pull( self._options, None, uri, str(ctx))
                if doc.is_fault(): raise OperationalError, doc.fault().reason()
                ctx = doc.root().find(pywsman.XML_NS_ENUMERATION,
                                                        "EnumerationContext")
                dicts = self._parseXml(''.join(l.strip() \
                                                for l in str(doc).split('\n')))
                if not descr:
                    if '*' in props: props = dicts[0].keys()
                    descr=tuple([(p,8,None,None,None,None,None) for p in props])
                for dict in dicts:
                    rows.append(tuple([self._convert(dict.get(p[0],
                                                        '')) for p in descr]))
            return descr, rows

        except OperationalError, e:
            raise OperationalError, e
        except Exception, e:
            raise OperationalError, e


### connection object

class win32comCnx(baseCnx):
    """
    This class represent an WS-Management Connection connection.
    """
    def __init__(self, scheme, port, path, user, password, host, namespace):
        self._host = host
        self._namespace = namespace
        self._cnx = None
        try:
            self._wsman = win32com.client.Dispatch("Wsman.Automation")
            flags = self._wsman.SessionFlagUseBasic()|\
                    self._wsman.SessionFlagCredUsernamePassword()
            options = self._wsman.CreateConnectionOptions()
            options.Username = user
            options.Password = password
            url = "%s://%s:%s%s"%(scheme, host, port, path)
            self._cnx = self._wsman.CreateSession(url, flags, options)
            doc = self._cnx.Identify()
        except Exception, e:
            raise OperationalError,"Access denied for user '%s' to '%s'"%(user,
                                                                            url)
        if 'Microsoft' in str(doc):
            self._dialect = "http://schemas.microsoft.com/wbem/wsman/1/WQL"
        else:
            self._dialect = "http://www.w3.org/TR/1999/REC-xpath-19991116"

    def _execute(self, query):
        """
        Execute WQL query and fetch first row
        """
        try:
            props, classname, kbs = WQLPAT.match(query).groups('')
            props = props.replace(' ','').split(',')
            if ('*' in props) or ('__path' in props) or ('__namespace' in props):
                flags = self._wsman.EnumerationFlagReturnObjectAndEPR()
            else:
                flags = 0
            if self._dialect == "http://schemas.microsoft.com/wbem/wsman/1/WQL":
                if self._namespace.startswith('http'):
                    uri = self._namespace + '/*'
                else:
                    uri="http://schemas.microsoft.com/wbem/wsman/1/wmi/%s/*"%\
                                                                self._namespace
            else:
                if self._namespace.startswith('http'):
                    uri = self._namespace + '/' + classname
                else:
                    uri = "http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2/"+\
                                                                    classname
            objSet = self._cnx.Enumerate(uri, query, self._dialect, flags)
            rows = []
            descr = None
            while not objSet.AtEndOfStream:
                pdict = {}
                dicts = self._parseXml(objSet.ReadItem())
                if not descr:
                    if '*' in props: props = dicts[0].keys()
                    descr=tuple([(p,8,None,None,None,None,None) for p in props])
                for dict in dicts:
                    rows.append(tuple([self._convert(dict.get(p[0],
                                                        '')) for p in descr]))
            return descr, rows

        except OperationalError, e:
            raise OperationalError, e
        except Exception, e:
            raise
            raise OperationalError, e

# connects to a WS-Management CIMOM
def Connect(scheme='http',port=5985,path='/wsman',user='',password='',host='',
                namespace='http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2'):

    """
    Constructor for creating a connection to the WS-Management. Returns
    a WS-Management Connection object. Paremeters are as follows:

    scheme        http or https
    port          port
    path          path
    user          user to connect as
    password      user's password
    host          host name
    namespace     namespace

    Examples:
    con = pywsmandb.connect(scheme='https',
                port=5986,
                path='/wsman'
                user='user',
                password='P@ssw0rd'
                host='localhost',
                namespace='http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2')
    """

    if platform.system() == 'Windows':
        return win32comCnx(scheme, port, path, user, password, host, namespace)
    else:
        return pywsmanCnx(scheme, port, path, user, password, host, namespace)

connect = Connection = Connect

__all__ = [ 'BINARY', 'Binary', 'Connect', 'Connection', 'DATE',
    'Date', 'Time', 'Timestamp', 'DateFromTicks', 'TimeFromTicks',
    'TimestampFromTicks', 'DataError', 'DatabaseError', 'Error',
    'FIELD_TYPE', 'IntegrityError', 'InterfaceError', 'InternalError',
    'MySQLError', 'NULL', 'NUMBER', 'NotSupportedError', 'DBAPITypeObject',
    'OperationalError', 'ProgrammingError', 'ROWID', 'STRING', 'TIME',
    'TIMESTAMP', 'Warning', 'apilevel', 'connect', 'paramstyle','threadsafety']
