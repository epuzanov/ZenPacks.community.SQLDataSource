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
__version__ = '1.1.0'

import threading
from xml.dom.minidom import parseString
from xml.dom.minicompat import NodeList
import datetime
import re
DTPAT = re.compile(r'^(\d{4})-?(\d{2})-?(\d{2})T?(\d{2}):?(\d{2}):?(\d{2})\.?(\d+)?([+|-]\d{2}\d?)?:?(\d{2})?')
WQLPAT = re.compile("^\s*SELECT\s+(?P<props>.+)\s+FROM\s+(?P<cn>\S+)(?:\s+WHERE\s+(?P<kbs>.+))?", re.I)

import sys
if sys.platform == 'win32':
    try:
        import win32com.client
    except:
        raise StandardError, "Can't import win32com.client module. Please, install 'Python Extensions for Windows' first."
else:
    try:
        import pywsman
    except:
        raise StandardError, "Can't import pywsman module. Please, install pywsman first."

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
        self._ctx = None
        self._cOpts = None
        self._uri = None

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
        if self._ctx:
            self.connection._release(self)
        self._cOpts = None
        self._uri = None
        del self._rows[:]

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
        self.rownumber = -1

        # for this method default value for params cannot be None,
        # because None is a valid value for format string.

        if (args != () and len(args) != 1):
            raise TypeError, "execute takes 1 or 2 arguments (%d given)" % (len(args) + 1,)

        if args != ():
            operation = operation%args[0]

        try:
            self.connection._execute(self, operation)

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
        return self.connection._fetchone(self)

    def fetchmany(self, size=None):
        """Fetch up to size rows from the cursor. Result set may be smaller
        than size. If size is not defined, cursor.arraysize is used."""
        self._check_executed()
        if not size: size = self.arraysize
        results = []
        row = self.connection._fetchone(self)
        while size and row:
            results.append(row)
            size -= 1
            if size: row = self.connection._fetchone(self)
        return results

    def fetchall(self):
        """Fetchs all available rows from the cursor."""
        self._check_executed()
        results = []
        row = self.connection._fetchone(self)
        while row:
            results.append(row)
            row = self.connection._fetchone(self)
        return results

    def next(self):
        """Fetches a single row from the cursor. None indicates that
        no more rows are available."""
        row = self.connection._fetchone(self)
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

class baseCnx:
    """
    This base class represent an WS-Management Connection connection.
    """

    def _parseXml(self, xmlroot, upperkey=True):
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
            cl = epr.firstChild.firstChild.nodeValue.rsplit('/', 1)[1]
            pdict = {'__CLASS': cl}
            kb=dict([(s.getAttributeNode('Name').nodeValue,s.firstChild.nodeValue
                ) for s in epr.getElementsByTagNameNS(XML_NS_WS_MAN,'Selector')])
            if kb:
                pdict['__NAMESPACE'] = kb.pop('__cimnamespace', 'root/cimv2')
                pdict['__PATH'] = '%s:%s.%s'%(pdict['__NAMESPACE'], cl,
                            ','.join(['%s="%s"'%kv for kv in kb.iteritems()]))
            for prop in item.firstChild.childNodes:
                pName = upperkey and prop.localName.upper() or prop.localName
                if prop.hasChildNodes:
                    value = prop.firstChild
                    if value: value = value.nodeValue
                else: value = None
                if pName in pdict:
                    if type(pdict[pName]) != list:
                        pdict[pName] = [pdict[pName],]
                    pdict[pName].append(value)
                else:
                    pdict[pName] = value
            dicts.append(pdict)
        return dicts


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


    def _parseIntrinsic(self, xmlroot, ns, pList):
        descr = []
        pTypes = {}
        maxLen = {}
        doc = parseString(xmlroot)
        for prop in doc.getElementsByTagNameNS(ns, 'property'):
            pName = prop.childNodes[1].firstChild.nodeValue
            pType = prop.childNodes[3].firstChild.nodeValue
            if not pType.strip():
                is_array = True
                pType = prop.childNodes[3].firstChild.nextSibling.firstChild.nodeValue
            else: is_array = False
            pTypes[pName] = self._parseType(pType, is_array)
            for qual in prop.getElementsByTagNameNS(ns, 'qualifier'):
                qName = qual.childNodes[1].firstChild.nodeValue
                if qName.lower() != 'maxlen': continue
                maxLen[pName] = int(qual.childNodes[5].firstChild.nodeValue)
        if not pList:
            pList = pTypes.keys()
            pList.extend(['__PATH', '__CLASS', '__NAMESPACE'])
        for pName in pList:
            descr.append((pName, pTypes.get(pName, 8), maxLen.get(pName, None),
                        maxLen.get(pName, None), None, None, None)) 
        return descr


    def _detectType(self, value):
        """
        Try to detect CIM types.
        """
        if hasattr(value, '__iter__'):
            return CIM_FLAG_ARRAY|self._convert(value[0])
        value = str(value).strip()
        if value.startswith('-') and value[1:].isdigit(): return CIM_SINT64
        if value.isdigit(): return CIM_UINT64
        if value.replace('.', '', 1).isdigit(): return CIM_REAL64
        if value.lower() == 'false': return CIM_BOOLEAN
        if value.lower() == 'true': return CIM_BOOLEAN
        if DTPAT.match(value): return CIM_DATETIME
        return CIM_STRING


    def _convert(self, value, pType=None):
        """
        Convert CIM types to Python standard types.
        """
        if not value or str(value).upper() == 'NULL': return None
        if pType > CIM_FLAG_ARRAY:
            if not hasattr(value, '__iter__'): value = [value]
            return [self._convert(v, pType - CIM_FLAG_ARRAY) for v in value]
        if pType == CIM_UINT8: return int(value)
        if pType == CIM_UINT16: return int(value)
        if pType == CIM_UINT32: return int(value)
        if pType == CIM_UINT64: return long(value)
        if pType == CIM_SINT8: return int(value)
        if pType == CIM_SINT16: return int(value)
        if pType == CIM_SINT32: return int(value)
        if pType == CIM_SINT64: return long(value)
        if pType == CIM_REAL32: return float(value)
        if pType == CIM_REAL64: return float(value)
        if pType == CIM_BOOLEAN:
            if value.lower() == 'true': return True
            else: return False
        if pType == CIM_DATETIME:
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
    def __init__(self, *args, **kwargs):
        self._lock = threading.RLock()
        self._host = kwargs.get('host', 'localhost')
        self._namespace = kwargs.get('namespace', 'root/cimv2')
        self._cnx = pywsman.Client(self._host, int(kwargs.get('port', 5985)),
                    kwargs.get('path', '/wsman'), kwargs.get('scheme', 'http'),
                    kwargs.get('user', ''), kwargs.get('password', ''))
        doc = self._cnx.identify( pywsman.ClientOptions() )
        if not doc:
            raise InterfaceError,"Access denied for user '%s' to '%s://%s:%s%s'"%(
                    kwargs.get('user', ''), kwargs.get('scheme', 'http'),
                    kwargs.get('host', 'localhost'), kwargs.get('port', 5985),
                    kwargs.get('path', '/wsman'))
        if 'Microsoft' in str(doc):
            self._dialect = pywsman.WSM_WQL_FILTER_DIALECT
            if not self._namespace.startswith('http'):
                self._namespace="http://schemas.microsoft.com/wbem/wsman/1/wmi/"\
                                + self._namespace
        elif 'Openwsman' in str(doc):
            self._dialect = pywsman.WSM_CQL_FILTER_DIALECT
            if not self._namespace.startswith('http'):
                self._namespace="http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2"
        else:
            self._dialect = None
            if not self._namespace.startswith('http'):
                self._namespace="http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2"


    def _execute(self, cursor, query):
        """
        Execute WQL query and fetch first row
        """
        if self._cnx is None:
            raise InterfaceError, "Connection closed."
        self._lock.acquire()
        try:
            try:
                props, classname, kbs = WQLPAT.match(query).groups('')
                props = props.replace(' ','').split(',')
                if '*' in props: props.remove('*')
                uProps = map(lambda v: str(v).upper(), props)
                self._release(cursor)
                del cursor._rows[:]
                cursor._cOpts = None
                cursor._cOpts = pywsman.ClientOptions()
                if not uProps or ('__PATH' in uProps) or ('__NAMESPACE' in uProps):
                    cursor._cOpts.set_flags(pywsman.FLAG_ENUMERATION_ENUM_OBJ_AND_EPR)
                else:
                    cursor._cOpts.clear_flags(pywsman.FLAG_ENUMERATION_ENUM_OBJ_AND_EPR)
                fltr = pywsman.Filter()
                if self._dialect == pywsman.WSM_WQL_FILTER_DIALECT:
                    fltr.simple(self._dialect, query)
                    classname = '*'
                elif self._dialect == pywsman.WSM_CQL_FILTER_DIALECT:
                    fltr.simple(self._dialect, query)
                    iuri = pywsman.XML_NS_CIM_INTRINSIC + '/' + classname
                    doc = self._cnx.invoke( cursor._cOpts, iuri, 'GetClass' )
                    if doc.is_fault():raise OperationalError,doc.fault().reason()
                    cursor.description=self._parseIntrinsic(str(doc),iuri,props)
                else: fltr = None
                cursor._uri = self._namespace + '/' + classname
                doc = self._cnx.enumerate( cursor._cOpts, fltr, cursor._uri )
                if doc.is_fault(): raise OperationalError, doc.fault().reason()
                root = doc.root()
                cursor._ctx = root.find(pywsman.XML_NS_ENUMERATION,
                                                        "EnumerationContext")
                if cursor.description: return
                doc = self._cnx.pull( cursor._cOpts, None, cursor._uri,
                                                            str(cursor._ctx))
                if doc.is_fault(): raise OperationalError, doc.fault().reason()
                cursor._ctx = doc.root().find(pywsman.XML_NS_ENUMERATION,
                                                        "EnumerationContext")
                dicts = self._parseXml(
                    ''.join([l.strip() for l in str(doc).split('\n')]), False)
                if not props:
                    props = dicts[0].keys()
                pDict = dict(zip(map(lambda v: str(v).upper(),dicts[0].keys()),
                                                            dicts[0].values()))
                cursor.description=tuple([(p,self._detectType(pDict.get(
                    p.upper(),'')),None,None,None,None,None) for p in props])
                for pDict in dicts:
                    pDict = dict(zip(map(lambda v: str(v).upper(),pDict.keys()),
                                                                pDict.values()))
                    cursor._rows.append(tuple([self._convert(pDict.get(
                    p[0].upper(), None),p[1]) for p in cursor.description]))
                cursor.rownumber = 0
            except OperationalError, e:
                raise OperationalError, e
            except Exception, e:
                raise OperationalError, e
        finally:
            self._lock.release()


    def _fetchone(self, cursor):
        if cursor._rows:
            cursor.rownumber += 1
            return cursor._rows.pop(0)
        if not cursor._ctx: return None
        self._lock.acquire()
        try:
            try:
                doc = self._cnx.pull(cursor._cOpts, None, cursor._uri,
                                                            str(cursor._ctx))
                if doc.is_fault(): raise OperationalError, doc.fault().reason()
                cursor._ctx = doc.root().find(pywsman.XML_NS_ENUMERATION,
                                                        "EnumerationContext")
                dicts = self._parseXml(
                            ''.join([l.strip() for l in str(doc).split('\n')]))
                for pDict in dicts:
                    cursor._rows.append(tuple([self._convert(pDict.get(
                        p[0].upper(), ''), p[1]) for p in cursor.description]))
                if not cursor._rows: return None
                cursor.rownumber += 1
                return cursor._rows.pop(0)
            except OperationalError, e:
                raise OperationalError, e
            except Exception, e:
                raise OperationalError, e
        finally:
            self._lock.release()


    def _release(self, cursor):
        if not cursor._ctx: return
        self._lock.acquire()
        try:
            try:
                doc = self._cnx.release( cursor._cOpts, cursor._uri,
                                                            str(cursor._ctx))
                if doc.is_fault(): raise OperationalError, doc.fault().reason()
                cursor._ctx = None

            except OperationalError, e:
                raise OperationalError, e
            except Exception, e:
                raise OperationalError, e
        finally:
            self._lock.release()



### connection object

class win32comCnx(baseCnx):
    """
    This class represent an WS-Management Connection connection.
    """
    def __init__(self, *args, **kwargs):
        self._lock = threading.RLock()
        self._host = kwargs.get('host', 'localhost')
        self._namespace = kwargs.get('namespace', 'root/cimv2')
        self._wsman = None
        self._cnx = None
        try:
            self._wsman = win32com.client.Dispatch("Wsman.Automation")
            flags = self._wsman.SessionFlagUseBasic()|\
                    self._wsman.SessionFlagCredUsernamePassword()
            options = self._wsman.CreateConnectionOptions()
            options.Username = kwargs.get('user', '')
            options.Password = kwargs.get('password', '')
            url = "%s://%s:%s%s"%(kwargs.get('scheme', 'http'), self._host,
                    int(kwargs.get('port', 5985)), kwargs.get('path', '/wsman'))
            self._cnx = self._wsman.CreateSession(url, flags, options)
            doc = self._cnx.Identify()
        except Exception, e:
            self._wsman = None
            self._cnx = None
            raise OperationalError,"Access denied for user '%s' to '%s'"%(
                                                    kwargs.get('user', ''), url)
        if 'Microsoft' in str(doc):
            self._dialect = "http://schemas.microsoft.com/wbem/wsman/1/WQL"
            if not self._namespace.startswith('http'):
                self._namespace="http://schemas.microsoft.com/wbem/wsman/1/wmi/"\
                                + self._namespace
        elif 'Openwsman' in str(doc):
            self._dialect = "http://schemas.dmtf.org/wbem/cql/1/dsp0202.pdf"
            if not self._namespace.startswith('http'):
                self._namespace="http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2"
        else:
            self._dialect = None
            if not self._namespace.startswith('http'):
                self._namespace="http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2"


    def _execute(self, cursor, query):
        """
        Execute WQL query and fetch first row
        """
        if self._cnx is None:
            raise InterfaceError, "Connection closed."
        self._lock.acquire()
        try:
            try:
                props, classname, kbs = WQLPAT.match(query).groups('')
                props = props.replace(' ','').split(',')
                if '*' in props: props.remove('*')
                del cursor._rows[:]
                uProps = map(lambda v: str(v).upper(), props)
                if not uProps or ('__PATH' in uProps) or ('__NAMESPACE' in uProps):
                    flags = self._wsman.EnumerationFlagReturnObjectAndEPR()
                else:
                    flags = 0
                if self._dialect=="http://schemas.microsoft.com/wbem/wsman/1/WQL":
                    classname = '*'
                elif self._dialect=="http://schemas.dmtf.org/wbem/cql/1/dsp0202.pdf":
                    pass
#                    iuri="http://schemas.openwsman.org/wbem/wscim/1/intrinsic/" \
#                                                                    + classname
#                    params = 'something here'
#                    doc = self._cnx.Invoke( 'GetClass', iuri, params, flags )
#                    cursor.description=self._parseIntrinsic(str(doc),iuri,props)
                else:
                    query = None
                cursor._uri = self._namespace + '/' + classname
                cursor._pEnum = self._cnx.Enumerate(cursor._uri, query,
                                                        self._dialect, flags)
                if cursor.description or cursor._pEnum.AtEndOfStream: return
                dicts = self._parseXml(cursor._pEnum.ReadItem(), False)
                if not props:
                    props = dicts[0].keys()
                pDict = dict(zip(map(lambda v: str(v).upper(),dicts[0].keys()),
                                                            dicts[0].values()))
                cursor.description=tuple([(p,self._detectType(pDict.get(
                    p.upper(),'')),None,None,None,None,None) for p in props])
                for pDict in dicts:
                    pDict = dict(zip(map(lambda v: str(v).upper(),pDict.keys()),
                                                                pDict.values()))
                    cursor._rows.append(tuple([self._convert(pDict.get(
                    p[0].upper(), None),p[1]) for p in cursor.description]))
                cursor.rownumber = 0

            except OperationalError, e:
                raise OperationalError, e
            except Exception, e:
                raise OperationalError, e
        finally:
            self._lock.release()


    def _fetchone(self, cursor):
        if cursor._rows:
            cursor.rownumber += 1
            return cursor._rows.pop(0)
        if getattr(cursor._pEnum, 'AtEndOfStream', True): return None
        self._lock.acquire()
        try:
            try:
                dicts = self._parseXml(cursor._pEnum.ReadItem())
                for pDict in dicts:
                    cursor._rows.append(tuple([self._convert(pDict.get(
                        p[0].upper(), ''), p[1]) for p in cursor.description]))
                if not cursor._rows: return None
                cursor.rownumber += 1
                return cursor._rows.pop(0)
            except OperationalError, e:
                raise OperationalError, e
            except Exception, e:
                raise OperationalError, e
        finally:
            self._lock.release()


    def close(self):
        """
        Close connection to the WS-Management CIMOM. Implicitly rolls back
        """
        self._cnx = None
        self._wsman = None

# connects to a WS-Management CIMOM
def Connect(*args, **kwargs):

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

    if sys.platform == 'win32':
        return win32comCnx(*args, **kwargs)
    else:
        return pywsmanCnx(*args, **kwargs)

connect = Connection = Connect

__all__ = [ 'BINARY', 'Binary', 'Connect', 'Connection', 'DATE',
    'Date', 'Time', 'Timestamp', 'DateFromTicks', 'TimeFromTicks',
    'TimestampFromTicks', 'DataError', 'DatabaseError', 'Error',
    'FIELD_TYPE', 'IntegrityError', 'InterfaceError', 'InternalError',
    'NULL', 'NUMBER', 'NotSupportedError', 'DBAPITypeObject',
    'OperationalError', 'ProgrammingError', 'ROWID', 'STRING', 'TIME',
    'TIMESTAMP', 'Warning', 'apilevel', 'connect', 'paramstyle','threadsafety']
