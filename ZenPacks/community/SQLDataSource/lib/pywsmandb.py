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
__version__ = '2.0.6'

from xml.sax import handler, make_parser
try: from uuid import uuid
except: import uuid
import httplib, urllib2
from datetime import datetime, timedelta
import threading
import re
WQLPAT = re.compile("^\s*SELECT\s+(?P<props>.+)\s+FROM\s+(?P<cn>\S+)(?:\s+WHERE\s+(?P<kbs>.+))?", re.I)
ANDPAT = re.compile("\s+AND\s+", re.I)
DTPAT = re.compile(r'^(\d{4})-?(\d{2})-?(\d{2})T?(\d{2}):?(\d{2}):?(\d{2})\.?(\d+)?([+|-]\d{2}\d?)?:?(\d{2})?')

XML_NS_SOAP_1_2 = "http://www.w3.org/2003/05/soap-envelope"
XML_NS_ADDRESSING = "http://schemas.xmlsoap.org/ws/2004/08/addressing"
XML_NS_ENUMERATION = "http://schemas.xmlsoap.org/ws/2004/09/enumeration"
XML_NS_CIM_INTRINSIC = "http://schemas.openwsman.org/wbem/wscim/1/intrinsic"
XML_NS_WS_MAN = "http://schemas.dmtf.org/wbem/wsman/1/wsman.xsd"
XML_NS_CIM_CLASS = "http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2"
XML_NS_WIN32_CIM_CLASS = "http://schemas.microsoft.com/wbem/wsman/1/wmi"
XML_NS_OWBEM_CIM_CLASS = "http://schema.openwbem.org/wbem/wscim/1/cim-schema/2"
XML_NS_LINUX_CIM_CLASS = "http://sblim.sf.net/wbem/wscim/1/cim-schema/2"
XML_NS_OMC_CIM_CLASS = "http://schema.omc-project.org/wbem/wscim/1/cim-schema/2"
XML_NS_PG_CIM_CLASS = "http://schema.openpegasus.org/wbem/wscim/1/cim-schema/2"
ENUM_ACTION_ENUMERATE = "http://schemas.xmlsoap.org/ws/2004/09/enumeration/Enumerate"
ENUM_ACTION_PULL = "http://schemas.xmlsoap.org/ws/2004/09/enumeration/Pull"
ENUM_ACTION_RELEASE = "http://schemas.xmlsoap.org/ws/2004/09/enumeration/Release"
WSM_WQL_FILTER_DIALECT = "http://schemas.microsoft.com/wbem/wsman/1/WQL"
WSM_CQL_FILTER_DIALECT = "http://schemas.dmtf.org/wbem/cql/1/dsp0202.pdf"


INTR_REQ = """<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:wsa="http://schemas.xmlsoap.org/ws/2004/08/addressing" xmlns:wsman="http://schemas.dmtf.org/wbem/wsman/1/wsman.xsd" xmlns:n1="http://schemas.openwsman.org/wbem/wscim/1/intrinsic/%s">
<s:Header>
<wsa:Action s:mustUnderstand="true">http://schemas.openwsman.org/wbem/wscim/1/intrinsic/%s/GetClass</wsa:Action>
<wsa:To s:mustUnderstand="true">%s</wsa:To>
<wsman:ResourceURI s:mustUnderstand="true">http://schemas.openwsman.org/wbem/wscim/1/intrinsic/%s</wsman:ResourceURI>
<wsa:MessageID s:mustUnderstand="true">uuid:%s</wsa:MessageID>
<wsa:ReplyTo>
<wsa:Address>http://schemas.xmlsoap.org/ws/2004/08/addressing/role/anonymous</wsa:Address>
</wsa:ReplyTo>
</s:Header>
<s:Body>
<n1:GetClass_INPUT/>
</s:Body>
</s:Envelope>"""
XML_REQ = """<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:wsa="http://schemas.xmlsoap.org/ws/2004/08/addressing" xmlns:wsman="http://schemas.dmtf.org/wbem/wsman/1/wsman.xsd" xmlns:wsen="http://schemas.xmlsoap.org/ws/2004/09/enumeration">
<s:Header>
<wsa:Action s:mustUnderstand="true">%s</wsa:Action>
<wsa:To s:mustUnderstand="true">%s</wsa:To>
<wsman:ResourceURI s:mustUnderstand="true">%s</wsman:ResourceURI>
<wsa:MessageID s:mustUnderstand="true">uuid:%s</wsa:MessageID>
<wsa:ReplyTo>
<wsa:Address>http://schemas.xmlsoap.org/ws/2004/08/addressing/role/anonymous</wsa:Address>
</wsa:ReplyTo>
</s:Header>
<s:Body>
%s
</s:Body>
</s:Envelope>"""

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
    r = DTPAT.match(dtarg)
    if not r: return str(dtarg)
    tt = map(int, r.groups(0))
    if abs(tt[7]) > 30: minutes = tt[7]
    elif tt[7] < 0: minutes = 60 * tt[7] - tt[8]
    else: minutes = 60 * tt[7] + tt[8]
    return datetime(*tt[:7]) #+ timedelta(minutes=minutes)

TYPEFUNCT = {CIM_UINT8: int, CIM_UINT16: int, CIM_UINT32: int, CIM_UINT64: long,
            CIM_SINT8: int, CIM_SINT16: int, CIM_SINT32: int, CIM_SINT64: long,
            CIM_REAL32: float, CIM_REAL64: float, CIM_STRING: str,
            CIM_CHAR16: str, CIM_OBJECT: str, CIM_REFERENCE: str,
            CIM_BOOLEAN: (lambda v: v.lower() is 'true'),
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


### xml.sax content handler

class WSMHandler(handler.ContentHandler):

    def __init__(self, cursor):
        handler.ContentHandler.__init__(self)
        self._cur = cursor
        self._tag = None
        self.fault = None
        self._faulttext = ''
        self._pVal = ''
        self._pdict = {}
        self._selectors = []

    def _detectType(self, value):
        """
        Try to detect CIM types.
        """
        if hasattr(value, '__iter__'):
            return CIM_FLAG_ARRAY|self._detectType(value[0])
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
        return TYPEFUNCT.get(pType, str)(value)


    def startElementNS(self, name, qname, attrs):
        if name[0] in set((XML_NS_SOAP_1_2, XML_NS_ADDRESSING)): return
        if name == (XML_NS_WS_MAN, "Selector"):
            self._selectors.append(attrs._attrs.get((None,'Name'),name[1]))
        elif self._tag == 'Item': self._tag = name
        elif self._tag: self._pVal = ''
        elif name == (XML_NS_WS_MAN, "Item"): self._tag = 'Item'


    def characters(self, content):
        if content.strip(): self._pVal = str(content)


    def endElementNS(self, name, qname):
#        if name[0] in set((XML_NS_SOAP_1_2, XML_NS_ADDRESSING)): return
        if self._tag == name:
            self._tag = None
        elif self._tag:
            if self._cur.description: pName = str(name[1]).upper()
            else: pName = str(name[1])
            if pName not in self._pdict:
                self._pdict[pName] = self._pVal
            elif type(self._pdict[pName]) is list:
                self._pdict[pName].append(self._pVal)
            else:
                self._pdict[pName] = [self._pdict[pName], self._pVal]
        elif name == (XML_NS_WS_MAN, "Selector"):
            self._selectors.append('%s="%s"'%(self._selectors.pop(),self._pVal))
        elif name == (XML_NS_ENUMERATION, "EnumerationContext"):
            self._cur._enumCtx = self._pVal
        elif name == (XML_NS_WS_MAN, "ResourceURI"):
            self._pdict['__NAMESPACE'],self._pdict['__CLASS'] = \
                                                        self._pVal.rsplit('/',1)
        elif name == (XML_NS_WS_MAN, "SelectorSet"):
            self._pdict['__PATH'] = '%s.%s'%(self._pdict['__CLASS'],
                                            ','.join(self._selectors))
            del self._selectors[:]
        elif name == (XML_NS_WS_MAN, "Item"):
            if not self._cur.description:
                if self._cur._props: props = self._cur._props
                else: props = self._pdict.keys()
                pDict = dict(zip(map(lambda v: str(v).upper(),
                                    self._pdict.keys()), self._pdict.values()))
                self._cur.description=tuple([(p,self._detectType(pDict.get(
                    p.upper(),'')),None,None,None,None,None) for p in props])
                self._pdict.clear()
                self._pdict.update(pDict)
            for pname, kbval in self._cur._selectors.iteritems():
                pval = self._pdict.get(pname.upper(), '')
                if kbval == pval: continue
                self._pdict.clear()
                return
            self._cur._rows.append(tuple([self._convert(self._pdict.get(
                    p[0].upper(), ''), p[1]) for p in self._cur.description]))
            self._pdict.clear()
        elif name == (XML_NS_ENUMERATION, "EndOfSequence"):
            self._cur._enumCtx = None
        elif name == (XML_NS_SOAP_1_2, "Text"): self._faulttext = self._pVal
        elif name == (XML_NS_SOAP_1_2, "Fault"):
            self.fault = self._faulttext or 'Unknown Error'

### xml.sax intrinsic handler

class intrHandler(handler.ContentHandler):

    def __init__(self, cursor):
        handler.ContentHandler.__init__(self)
        self._tag = ''
        self._cur = cursor
        self._pName = ''
        self._qName = ''
        self._tVal = ''
        self._maxLen = None
        self._pType = 0
        self._descr = None

    def startElementNS(self, (ns, name), qname, attrs):
        if name not in ('property', 'qualifier'): return
        if name == 'property': self._tag = 'p'
        elif name == 'qualifier': self._tag = 'q'

    def characters(self, content):
        if not self._tag: return
        if content.strip(): self._tVal = unicode(content)


    def endElementNS(self, (ns, name), qname):
        if not self._tag: return
        if self._tag == 'p':
            if name == 'name': self._pName = self._tVal
            elif name == 'array':
                self._pType = CIM_FLAG_ARRAY|TYPEDICT.get(self._tVal, 0)
            elif name == 'type' and self._pType == 0:
                self._pType = TYPEDICT.get(self._tVal, 0)
            elif name == 'property':
                self._descr[self._pName.upper()] = (self._pName, self._pType,
                    self._maxLen, self._maxLen, None, None, None)
                self._maxLen = None
                self._pType = 0
                self._tag = ''
        elif self._tag == 'q':
            if name == 'name': self._qName = self._tVal.lower()
            elif name == 'value' and self._qName == 'maxlen':
                self._maxLen = int(self._tVal)
            elif name == 'qualifier': self._tag = 'p'

    def startDocument(self):
        self._descr = {'__PATH': ('__PATH',CIM_STRING,None,None,None,None,None),
            '__NAMESPACE': ('__NAMESPACE',CIM_STRING,None,None,None,None,None),
            '__CLASS': ('__CLASS',CIM_STRING,None,None,None,None,None),
            }

    def endDocument(self): 
        if not self._cur._props: self._cur.description = self._descr.values()
        else: self._cur.description = tuple([self._descr.get(p.upper(), (p,
            CIM_STRING,None,None,None,None,None)) for p in self._cur._props])
        self._descr = None


### HTTPSClientAuthHandler object

class HTTPSClientAuthHandler(urllib2.HTTPSHandler):
    def __init__(self, key, cert):
        urllib2.HTTPSHandler.__init__(self)
        self.key = key
        self.cert = cert
    def https_open(self, req):
        return self.do_open(self.getConnection, req)
    def getConnection(self, host):
        return httplib.HTTPSConnection(host, key_file=self.key, cert_file=self.cert)


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
        self._uri = ''
        self._enumCtx = None
        self._props = []
        self._selectors = {}
        self._rows = []
        self._parser = make_parser()
        self._parser.setFeature(handler.feature_namespaces, 1)
        self._parser.setContentHandler(WSMHandler(self))
        self._iparser = make_parser()
        self._iparser.setFeature(handler.feature_namespaces, 1)
        self._iparser.setContentHandler(intrHandler(self))


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
        if self._enumCtx:
            self.connection._release(self)
        del self._rows[:]
        self._selectors.clear()
        self._iparser = None
        self._parser = None

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
            self.connection._execute(self, operation.replace('\\', '\\\\'
                                                    ).replace('\\\\"', '\\"'))
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

class wsmanCnx:
    """
    This class represent an WS-Management Connection connection.
    """
    def __init__(self, *args, **kwargs):
        self._lock = threading.RLock()
        self._host = kwargs.get('host', 'localhost')
        self._scheme = kwargs.get('scheme', 'http')
        self._port=int(kwargs.get('port',self._scheme=='http' and 5985 or 5986))
        self._path = kwargs.get('path', '/wsman')
        self._dialect = {'WQL':WSM_WQL_FILTER_DIALECT,
                        'CQL':WSM_CQL_FILTER_DIALECT,
                        }.get(kwargs.get('dialect', '').upper(), '')
        self._namespace = kwargs.get('namespace', 'root/cimv2')
        self._url='%s://%s:%s%s'%(self._scheme,self._host,self._port,self._path)
        self._urlOpener = urllib2.build_opener()
        if 'user' in kwargs:
            passman = urllib2.HTTPPasswordMgrWithDefaultRealm()
            passman.add_password(None,self._url,kwargs['user'],
                                            kwargs.get('password', ''))
            authhandler = urllib2.HTTPBasicAuthHandler(passman)
            self._urlOpener.add_handler(authhandler)
        elif 'key_file' in kwargs and 'cert_file' in kwargs:
            sslauthhandler = HTTPSClientAuthHandler(kwargs['key_file'],
                                                    kwargs['cert_file'])
            self._urlOpener.add_handler(sslauthhandler)
        self._wsm_vendor = ''
        xml_repl = self._wsman_request("""<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:wsmid="http://schemas.dmtf.org/wbem/wsman/identity/1/wsmanidentity.xsd">
<s:Header/>
<s:Body>
<wsmid:Identify/>
</s:Body>
</s:Envelope>""").read()
        if 'ProductVendor' not in xml_repl:
            raise InterfaceError,"Access denied for user '%s' to '%s'"%(
                                                                uname,self._url)
        elif 'Microsoft' in xml_repl: self._wsm_vendor = 'Microsoft'
        elif 'Openwsman' in xml_repl: self._wsm_vendor = 'Openwsman'
        else: self._wsm_vendor = ''

    def _wsman_request(self, data, action = None):
        """Send SOAP+XML data over HTTP to the specified url. Return the
        response in XML.  Uses Python's build-in urllib2.
        """

        data = '<?xml version="1.0" encoding="utf-8" ?>\n%s'%data

        headers = { 'Content-type': 'application/soap+xml; charset="utf-8"',
                    'Content-length': len(data),
                    'User-Agent': 'pywsmandb',
                    }

        if action:
            headers['SOAPAction'] = action

        request = urllib2.Request(self._url, data, headers)

        tryLimit = 5
        xml_repl = None
        while not xml_repl:
            tryLimit -= 1
            try:
                xml_repl = self._urlOpener.open(request)
            except urllib2.HTTPError, arg:
                if arg.code in [401, 504] and tryLimit > 0: xml_repl = None
                else: raise InterfaceError('HTTP error: %s' % arg.code)
            except urllib2.URLError, arg:
                if arg.reason[0] in [32, 104] and tryLimit > 0: xml_repl = None
                else: raise InterfaceError('socket error: %s' % arg.reason)
        if self._wsm_vendor == 'Openwsman': xml_repl.readline()
        return xml_repl


    def _execute(self, cursor, query):
        """
        Execute Query
        """
        try:
            props, classname, where = WQLPAT.match(query).groups('')
        except:
            raise ProgrammingError, "Syntax error in the query statement."
        cursor._selectors.clear()
        if where:
            try:
                cursor._selectors.update(
                    eval('(lambda **kws:kws)(%s)'%ANDPAT.sub(',', where))
                    )
                if [v for v in cursor._selectors.values() if type(v) is list]:
                    kbkeys = ''
                    if props != '*':
                        kbkeys = ',%s'%','.join(cursor._selectors.keys())
                    query = 'SELECT %s%s FROM %s'%(props, kbkeys, classname)
                elif self._dialect: cursor._selectors.clear()
            except: cursor._selectors.clear()
        if props == '*': cursor._props = []
        else:cursor._props=[p for p in set(props.replace(' ','').split(','))]
        self._lock.acquire()
        try:
            try:
                if self._wsm_vendor == 'Microsoft':
                    fltr = '\n<wsman:Filter Dialect="%s">%s</wsman:Filter>'%(
                                                WSM_WQL_FILTER_DIALECT, query)
                    if not self._namespace.startswith('http'):
                        cursor._uri = '/'.join((XML_NS_WIN32_CIM_CLASS,
                        self._namespace, '*'))
                    else: cursor._uri = '/'.join((self._namespace, '*'))
                else:
                    fltr = ''
                    if not self._namespace.startswith('http'):
                        cursor._uri = '/'.join(({'CIM': XML_NS_CIM_CLASS,
                                            'OpenWBEM': XML_NS_OWBEM_CIM_CLASS,
                                            'Linux': XML_NS_LINUX_CIM_CLASS,
                                            'OMC': XML_NS_OMC_CIM_CLASS,
                                            'PG': XML_NS_PG_CIM_CLASS,
                                            }.get(classname.split('_', 1)[0],
                                            XML_NS_CIM_CLASS), classname))
                    else: cursor._uri = '/'.join((self._namespace, classname))
                    if self._dialect:
                        fltr='\n<wsman:Filter Dialect="%s">%s</wsman:Filter>'%(
                                                            self._dialect,query)
                    if self._wsm_vendor == 'Openwsman':
                        xml_repl = self._wsman_request(INTR_REQ%(classname,
                            classname, self._url, classname, uuid.uuid4()),
                            '/'.join((XML_NS_CIM_INTRINSIC,classname,'GetClass')
                            ))
                        cursor._iparser.parse(xml_repl)
                xml_repl = self._wsman_request(XML_REQ%(ENUM_ACTION_ENUMERATE,
                    self._url, cursor._uri, uuid.uuid4(), """<wsen:Enumerate>
<wsman:EnumerationMode>EnumerateObjectAndEPR</wsman:EnumerationMode>%s
</wsen:Enumerate>"""%fltr), ENUM_ACTION_ENUMERATE)
                cursor._parser.parse(xml_repl)
                if not cursor._enumCtx: return
                if not cursor.description:
                    xml_repl = self._wsman_request(XML_REQ%(ENUM_ACTION_PULL,
                        self._url, cursor._uri, uuid.uuid4(), """<wsen:Pull>
<wsen:EnumerationContext>%s</wsen:EnumerationContext>
</wsen:Pull>"""%cursor._enumCtx), ENUM_ACTION_PULL)
                    cursor._parser.parse(xml_repl)
            except InterfaceError, e:
                raise InterfaceError, e
            except OperationalError, e:
                raise OperationalError, e
            except Exception, e:
                raise OperationalError, e
        finally:
            self._lock.release()


    def _fetchone(self, cursor):
        self._lock.acquire()
        try:
            try:
                while not cursor._rows and cursor._enumCtx:
                    xml_repl = self._wsman_request(XML_REQ%(ENUM_ACTION_PULL,
                        self._url, cursor._uri, uuid.uuid4(), """<wsen:Pull>
<wsen:EnumerationContext>%s</wsen:EnumerationContext>
</wsen:Pull>"""%cursor._enumCtx), ENUM_ACTION_PULL)
                    cursor._parser.parse(xml_repl)
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
        if not cursor._enumCtx: return
        self._lock.acquire()
        try:
            try:
                xml_repl = self._wsman_request(XML_REQ%(ENUM_ACTION_RELEASE,
                    self._url, cursor._uri, uuid.uuid4(), """<wsen:Release>
<wsen:EnumerationContext>%s</wsen:EnumerationContext>
</wsen:Release>"""%cursor._enumCtx), ENUM_ACTION_RELEASE)
                cursor._enumCtx = None

            except OperationalError, e:
                raise OperationalError, e
            except Exception, e:
                raise OperationalError, e
        finally:
            self._lock.release()

    def __del__(self):
        self.close()

    def close(self):
        """
        Close connection to the WBEM CIMOM. Implicitly rolls back
        """
        return

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
    dialect       query dialect
    key_file      key file for Certificate based Authorization
    cert_file     cert file for Certificate based Authorization

    Examples:
    con = pywsmandb.connect(scheme='https',
                port=5986,
                path='/wsman'
                user='user',
                password='P@ssw0rd'
                host='localhost',
                namespace='http://schemas.dmtf.org/wbem/wscim/1/cim-schema/2')
                dialect='CQL'
    """

    return wsmanCnx(*args, **kwargs)

connect = Connection = Connect

__all__ = [ 'BINARY', 'Binary', 'Connect', 'Connection', 'DATE',
    'Date', 'Time', 'Timestamp', 'DateFromTicks', 'TimeFromTicks',
    'TimestampFromTicks', 'DataError', 'DatabaseError', 'Error',
    'FIELD_TYPE', 'IntegrityError', 'InterfaceError', 'InternalError',
    'NULL', 'NUMBER', 'NotSupportedError', 'DBAPITypeObject',
    'OperationalError', 'ProgrammingError', 'ROWID', 'STRING', 'TIME',
    'TIMESTAMP', 'Warning', 'apilevel', 'connect', 'paramstyle','threadsafety']
