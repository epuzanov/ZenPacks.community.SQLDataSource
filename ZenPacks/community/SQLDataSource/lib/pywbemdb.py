#***************************************************************************
# pywbemdb - A DB API v2.0 compatible interface to WBEM.
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
__version__ = '2.3.1'

import socket
from xml.sax import xmlreader, handler, make_parser, SAXParseException
import httplib, base64
import threading
try:
    from cStringIO import StringIO
except ImportError:
    from StringIO import StringIO
from datetime import datetime, timedelta
import re
WQLPAT = re.compile("^\s*SELECT\s+(?P<props>.+)\s+FROM\s+(?P<cn>\S+)(?:\s+WHERE\s+(?P<kbs>.+))?", re.I)
ANDPAT = re.compile("\s+AND\s+", re.I)
DTPAT = re.compile(r'^(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2})\.(\d{6})([+|-]\d{3})')
TDPAT = re.compile(r'^(\d{8})(\d{2})(\d{2})(\d{2})\.(\d{6})')


XML_REQ = """<?xml version="1.0" encoding="utf-8" ?>
<CIM CIMVERSION="2.0" DTDVERSION="2.0">
<MESSAGE ID="1001" PROTOCOLVERSION="1.0">
<SIMPLEREQ>
<IMETHODCALL NAME="%s">
<LOCALNAMESPACEPATH>
<NAMESPACE NAME="%s"/>
</LOCALNAMESPACEPATH>%s
</IMETHODCALL>
</SIMPLEREQ>
</MESSAGE>
</CIM>"""
EXECQUERY_IPARAM = """
<IPARAMVALUE NAME="Query">
<VALUE>%s</VALUE>
</IPARAMVALUE>
<IPARAMVALUE NAME="QueryLanguage">
<VALUE>%s</VALUE>
</IPARAMVALUE>"""
CLNAME_IPARAM = """
<IPARAMVALUE NAME="ClassName">
<CLASSNAME NAME="%s"/>
</IPARAMVALUE>"""
QUALS_IPARAM = """
<IPARAMVALUE NAME="IncludeQualifiers">
<VALUE>%s</VALUE>
</IPARAMVALUE>
<IPARAMVALUE NAME="LocalOnly">
<VALUE>FALSE</VALUE>
</IPARAMVALUE>"""
PL_IPARAM = """
<IPARAMVALUE NAME="PropertyList">
<VALUE.ARRAY>
<VALUE>%s</VALUE>
</VALUE.ARRAY>
</IPARAMVALUE>"""

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
TYPEDICT = {'sint8':CIM_SINT8, 'uint8':CIM_UINT8, 
            'sint16':CIM_SINT16, 'uint16':CIM_UINT16,
            'sint32':CIM_SINT32, 'uint32':CIM_UINT32,
            'sint64':CIM_SINT64, 'uint64':CIM_UINT64,
            'real32':CIM_REAL32, 'real64':CIM_REAL64,
            'string': CIM_STRING, 'char16':CIM_CHAR16,
            'boolean':CIM_BOOLEAN, 'datetime':CIM_DATETIME,
            'object':CIM_OBJECT, 'reference':CIM_REFERENCE}

def _datetime(dtarg):
    """
    Convert string to datetime.
    """
    s = DTPAT.match(dtarg)
    if s is not None:
        tt = map(int, s.groups(0))
        return datetime(*tt[:7]) #+ timedelta(minutes=tt[7])
    s = TDPAT.match(dtarg)
    if s is None: return str(dtarg)
    return timedelta(**dict(zip(('days','hours','minutes','microseconds'),
                                                    map(int, s.groups(0)))))

TYPEFUNCT = {CIM_UINT8: int, CIM_UINT16: int, CIM_UINT32: int, CIM_UINT64: long,
            CIM_SINT8: int, CIM_SINT16: int, CIM_SINT32: int, CIM_SINT64: long,
            CIM_REAL32: float, CIM_REAL64: float, CIM_STRING: str,
            CIM_CHAR16: str, CIM_OBJECT: str, CIM_REFERENCE: str,
            CIM_BOOLEAN: lambda v: str(v).lower() == 'true',
            CIM_DATETIME: _datetime}


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


### xml.sax content handler

class CIMHandler(handler.ContentHandler):

    def __init__(self, cursor):
        handler.ContentHandler.__init__(self)
        self._in=['IRETURNVALUE','IMETHODRESPONSE','SIMPLERSP','MESSAGE','CIM']
        self._cur = cursor
        self._con = cursor._connection
        self._methodname = cursor._dialect and 'ExecQuery' or 'EnumerateInstances'
        self._namespace = cursor._namespace
        self._qName = ''
        self._rName = ''
        self._rClass = ''
        self._pName = ''
        self._pType = ''
        self._pVal = []
        self._pdict = {}
        self._kbs = []
        self._maxlen = {}


    def startElement(self, name, attrs):
        if name in ('VALUE.ARRAY', 'VALUE.REFERENCE', 'VALUE'): return
        if self._in:
            tag = self._in.pop()
            if name == tag:
                if name != 'IMETHODRESPONSE' or str(attrs._attrs.get('NAME',
                                        '')) == self._methodname: return
                raise InterfaceError(0, 'Expecting attribute NAME=%s, got %s'%(
                    self._methodname, str(attrs._attrs.get('NAME', ''))))
            elif name == 'ERROR':
                errcode = int(attrs._attrs.get('CODE', 0))
                raise InterfaceError(errcode, attrs._attrs.get('DESCRIPTION',
                            'Error code %s'%errcode))
            else:
                raise InterfaceError(0,'Expecting %s element, got %s'%(tag,name))
        elif name == 'PROPERTY':
            self._pName = str(attrs._attrs.get('NAME', ''))
            self._pType = TYPEDICT.get(attrs._attrs.get('TYPE', ''), CIM_STRING)
            del self._pVal[:]
            if type(self._cur.description) is tuple: return
            self._cur.description.append((self._pName,
                                    self._pType, None, None, None, None, None))
        elif name == 'PROPERTY.ARRAY':
            self._pName = str(attrs._attrs.get('NAME', ''))
            self._pType = TYPEDICT.get(attrs._attrs.get('TYPE', ''), CIM_STRING)
            del self._pVal[:]
            if type(self._cur.description) is tuple: return
            self._cur.description.append((self._pName,
                            0x2000|self._pType, None, None, None, None, None))
        elif name == 'KEYVALUE':
            self._pType=TYPEDICT.get(attrs._attrs.get('VALUETYPE',''),CIM_STRING)
            del self._pVal[:]
        elif name == 'KEYBINDING':
            self._pName = str(attrs._attrs.get('NAME', ''))
        elif name == 'INSTANCE':
            if not self._cur.description: self._cur.description = []
        elif name == 'INSTANCENAME':
            if self._rName:
                self._rClass = str(attrs._attrs.get('CLASSNAME', ''))
            else:
                self._pdict['__CLASS'] = str(attrs._attrs.get('CLASSNAME', ''))
                self._pdict['__NAMESPACE'] = self._namespace
        elif name == 'PROPERTY.REFERENCE':
            self._rName = str(attrs._attrs.get('NAME', ''))
            self._rClass = str(attrs._attrs.get('REFERENCECLASS', ''))
            self._pType = CIM_STRING
            del self._pVal[:]
            if type(self._cur.description) is tuple: return
            self._cur.description.append((self._pName,
                                    self._pType, None, None, None, None, None))


    def characters(self, content):
        if not content.strip(): return
        if content == 'NULL': val = None
        else:
            try: val = TYPEFUNCT.get(self._pType, str)(content)
            except ValueError: val = unicode(content)
        self._pVal.append(val)


    def endElement(self, name):
        if name in ('VALUE.ARRAY', 'VALUE.REFERENCE', 'VALUE', 'KEYVALUE',
                                                'PROPERTY.REFERENCE'): return
        if name == 'PROPERTY':
            if len(self._pVal) == 1:
                self._pdict[self._pName.upper()] = self._pVal[0]
            elif not self._pVal:
                self._pdict[self._pName.upper()] = None
            elif len(self._pVal) > 1 and self._pType == STRING:
                self._pdict[self._pName.upper()]='\n'.join(map(str,self._pVal))
            else:
                self._pdict[self._pName.upper()] = self._pVal[0]
            del self._pVal[:]
        elif name == 'PROPERTY.ARRAY':
            self._pdict[self._pName.upper()] = self._pVal
            self._pVal = []
        elif name == 'KEYBINDING':
            if self._pType == STRING:
                self._kbs.append('%s="%s"'%(self._pName, self._pVal[0]))
            else:
                self._kbs.append('%s=%s'%(self._pName, self._pVal[0]))
        elif name == 'INSTANCE':
            if type(self._cur.description) is list:
                if self._cur._props:
                    pDct=dict([(p[0].upper(),p) for p in self._cur.description])
                    self._cur.description = [pDct.get(p.upper(), (p, CIM_STRING,
                        None,None,None,None,None)) for p in self._cur._props]
                else:self._cur.description.extend([(p,CIM_STRING,None,None,None,
                    None,None) for p in ['__CLASS', '__NAMESPACE', '__PATH']])
                self._cur.description = tuple(self._cur.description)
            for pname, kbval in self._cur._keybindings.iteritems():
                pval = self._pdict.get(pname.upper(), '')
                if kbval == pval: continue
                self._pdict.clear()
                return
            self._cur._rows.append(tuple([self._pdict.get(
                        p[0].upper(), None) for p in self._cur.description]))
            self._pdict.clear()
        elif name == 'INSTANCENAME':
            if self._rName:
                self._pdict[self._rName.upper()] = '%s.%s'%(self._rClass,
                                                            ','.join(self._kbs))
                self._rName = ''
                del self._pVal[:]
            else:
                self._pdict['__PATH'] = '%s.%s'%(self._pdict['__CLASS'],
                                                            ','.join(self._kbs))
            del self._kbs[:]

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
        self._connection = connection
        self._dialect = connection._dialect
        self._namespace = connection._namespace
        self.description = None
        self.rownumber = -1
        self.arraysize = 1
        self._rows = []
        self._props = []
        self._keybindings = {}

    @property
    def rowcount(self):
        """
        Returns number of rows affected by last operation. In case
        of SELECTs it returns meaningful information only after
        all rows has been fetched.
        """
        return len(self._rows)

    def _check_executed(self):
        if not self._connection:
            raise ProgrammingError("Cursor closed.")
        if not self._connection._conkwargs:
            raise ProgrammingError("Connection closed.")
        if not self.description:
            raise OperationalError("No data available. execute() first.")

    def __del__(self):
        self.close()

    def close(self):
        """
        Closes the cursor. The cursor is unusable from this point.
        """
        del self._rows[:]
        del self._props[:]
        self._keybindings.clear()
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
        if not self._connection._conkwargs:
            raise ProgrammingError("Connection closed.")
        self.description = None
        self.rownumber = -1
        del self._rows[:]
        self._keybindings.clear()
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
            props, classname, where = WQLPAT.match(operation).groups('')
        except:
            raise ProgrammingError("Syntax error in the query statement.")
        if where:
            try:
                self._keybindings.update(
                    eval('(lambda **kws:kws)(%s)'%ANDPAT.sub(',', where))
                    )
                if [v for v in self._keybindings.values() if type(v) is list]:
                    kbkeys = ''
                    if props != '*':
                        kbkeys = ',%s'%','.join(self._keybindings.keys())
                    operation = 'SELECT %s%s FROM %s'%(props, kbkeys, classname)
                elif self._dialect:
                    self._keybindings.clear()
            except: self._keybindings.clear()
        if props == '*': self._props = []
        else: self._props = [p for p in props.replace(' ','').split(',')]
        try:
            if self._dialect:
                xml_req = XML_REQ%('ExecQuery',
                    '"/>\n<NAMESPACE NAME="'.join(self._namespace.split('/')),
                    EXECQUERY_IPARAM%(EXECQUERY_IPARAM%operation,self._dialect))
            else:
                pLst = [p for p in set(self._props) \
                    if p.upper() not in ('__PATH','__CLASS','__NAMESPACE')]
                pLst.extend(self._keybindings.keys())
                xml_req = XML_REQ%('EnumerateInstances',
                    '"/>\n<NAMESPACE NAME="'.join(self._namespace.split('/')),
                    ''.join((CLNAME_IPARAM%classname,QUALS_IPARAM%'FALSE',
                    pLst and PL_IPARAM%'</VALUE>\n<VALUE>'.join(pLst) or '')))
            xml_resp = self._connection._wbem_request(xml_req)
            if xml_resp and good_sql:
                self._rows.append((1L,))
                self.description = (('1',CIM_UINT64,None,None,None,None,None),)
                self.rownumber = 0
                return
            xml_handler = CIMHandler(self)
            parser = make_parser()
            parser.setContentHandler(xml_handler)
            inpsrc = xmlreader.InputSource()
            inpsrc.setByteStream(StringIO(xml_resp))
            parser.parse(inpsrc)
            if self.description: self.rownumber = 0

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

class pywbemCnx:
    """
    This class represent an WBEM Connection connection.
    """
    def __init__(self, *args, **kwargs):
        self._connection = None
        self._timeout = float(kwargs.get('timeout', 60))
        self._dialect = kwargs.get('dialect', '').upper()
        self._scheme = str(kwargs.get('scheme', 'https')).lower()
        self._conkwargs = {
            'host':kwargs.get('host') or 'localhost',
            'port':int(kwargs.get('port',self._scheme=='http' and 5988 or 5989))}
        self._namespace = kwargs.get('namespace') or 'root/cimv2'
        self._headers = {'Content-type': 'application/xml; charset="utf-8"',
            'CIMOperation': 'MethodCall',
            'CIMMethod': self._dialect and 'ExecQuery' or 'EnumerateInstances',
            'CIMObject': self._namespace}
        if 'user' in kwargs:
            self._headers['Authorization'] = 'Basic %s'%base64.encodestring(
                '%s:%s'%(kwargs['user'], kwargs.get('password','')))[:-1]
        if self._scheme == 'https':
            if 'key_file' in kwargs and 'cert_file' in kwargs:
                self._conkwargs['key_file'] = kwargs['key_file']
                self._conkwargs['cert_file'] = kwargs['cert_file']
        self._lock = threading.Lock()

    def _wbem_request(self, data):
        """Send XML data over HTTP to the specified url. Return the
        response in XML.  Uses Python's build-in httplib.
        """

        oldtimeout = None
        try:
            self._lock.acquire()
            if self._scheme == 'https':
                self._connection = httplib.HTTPSConnection(**self._conkwargs)
            else:
                self._connection = httplib.HTTPConnection(**self._conkwargs)
            if hasattr(self._connection, 'timeout'):
                self._connection.timeout = self._timeout
            else:
                oldtimeout = socket.getdefaulttimeout()
                if oldtimeout != self._timeout:
                    socket.setdefaulttimeout(self._timeout)
            try:
                try:
                    if not getattr(self._connection, 'sock', None):
                        self._connection.connect()
                    self._connection.request('POST','/cimom',data,self._headers)
                except socket.error, arg:
                    if arg[0] != 104 and arg[0] != 32:
                        raise

                response = self._connection.getresponse()
                xml_resp = response.read()
                if response.status != 200:
                    if response.getheader('CIMError', None) is not None and \
                        response.getheader('PGErrorDetail', None) is not None:
                            import urllib
                            raise InterfaceError("CIMError: (%s, '%s')" %
                                (response.getheader('CIMError'),
                                 urllib.unquote(response.getheader('PGErrorDetail'))))
                    raise InterfaceError('HTTP error: %s'%str((response.status,
                                                            response.reason)))

                return xml_resp
            except SAXParseException, e:
                raise OperationalError("XML parsing error: %s" % e.getMessage())
            except httplib.BadStatusLine, arg:
                raise InterfaceError("The web server returned a bad status line: '%s'" % arg)
            except socket.error, arg:
                raise InterfaceError("Socket error: %s" % (arg,))
            except socket.sslerror, arg:
                raise InterfaceError("SSL error: %s" % (arg,))
        finally:
            if self._connection is not None:
                _connection, self._connection = self._connection, None
                _connection.close()
            if oldtimeout and oldtimeout != socket.getdefaulttimeout():
                socket.setdefaulttimeout(oldtimeout)
            self._lock.release()

    def __del__(self):
        self.close()

    def close(self):
        """
        Close connection to the WBEM CIMOM. Implicitly rolls back
        """
        if self._connection is not None:
            _connection, self._connection = self._connection, None
            _connection.close()
        self._conkwargs.clear()

    def commit(self):
        """
        Commit transaction which is currently in progress.
        """
        if not self._conkwargs:
            raise ProgrammingError("Connection closed.")

    def rollback(self):
        """
        Roll back transaction which is currently in progress.
        """
        if not self._conkwargs:
            raise ProgrammingError("Connection closed.")
        if self._connection is not None:
            _connection, self._connection = self._connection, None
            _connection.close()

    def cursor(self):
        """
        Return cursor object that can be used to make queries and fetch
        results from the database.
        """
        if not self._conkwargs:
            raise ProgrammingError("Connection closed.")
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
    timeout       query timeout in seconds
    dialect       query dialect
    key_file      key file for Certificate based Authorization
    cert_file     cert file for Certificate based Authorization

    Examples:
    con  =  pywbemdb.connect(scheme='https',
                            port=5989,
                            user='user',
                            password='P@ssw0rd'
                            host='localhost',
                            namespace='root/cimv2',
                            dialect='CQL'
                            )
    """

    return pywbemCnx(*args, **kwargs)

connect = Connection = Connect

__all__ = [ 'BINARY', 'Binary', 'Connect', 'Connection', 'DATE',
    'Date', 'Time', 'Timestamp', 'DateFromTicks', 'TimeFromTicks',
    'TimestampFromTicks', 'DataError', 'DatabaseError', 'Error',
    'FIELD_TYPE', 'IntegrityError', 'InterfaceError', 'InternalError',
    'NULL', 'NUMBER', 'NotSupportedError', 'DBAPITypeObject',
    'OperationalError', 'ProgrammingError', 'ROWID', 'STRING', 'TIME',
    'TIMESTAMP', 'Warning', 'apilevel', 'connect', 'paramstyle','threadsafety']
