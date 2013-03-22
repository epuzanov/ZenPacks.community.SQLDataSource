#***************************************************************************
# pywsmandb - A DB API v2.0 compatible interface to WS-Management.
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
try:
    from uuid import uuid
except ImportError:
    import uuid
from datetime import datetime, timedelta

import re
WQLPAT = re.compile("^\s*SELECT\s+(?P<props>.+)\s+FROM\s+(?P<cn>\S+)(?:\s+WHERE\s+(?P<kbs>.+))?", re.I)
ANDPAT = re.compile("\s+AND\s+", re.I)
DTPAT = re.compile(r'^(\d{4})-?(\d{2})-?(\d{2})T?(\d{2}):?(\d{2}):?(\d{2})\.?(\d+)?([+|-]\d{2}\d?)?:?(\d{2})?')
ACTIONPAT = re.compile(r'>(.*)</wsa:Action>')
VENDORPAT = re.compile("ProductVendor>([^<]*)<")

XML_NS_SOAP_1_2 = "http://www.w3.org/2003/05/soap-envelope"
XML_NS_ADDRESSING = "http://schemas.xmlsoap.org/ws/2004/08/addressing"
XML_NS_ENUMERATION = "http://schemas.xmlsoap.org/ws/2004/09/enumeration"
#XML_NS_CIM_INTRINSIC = "http://schemas.openwsman.org/wbem/wscim/1/intrinsic"
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

WQL_FILTER_TMPL = '\n<wsman:Filter Dialect="http://schemas.microsoft.com/wbem/wsman/1/WQL">%s</wsman:Filter>'
CQL_FILTER_TMPL = '\n<wsman:Filter Dialect="http://schemas.dmtf.org/wbem/cql/1/dsp0202.pdf">%s</wsman:Filter>'

ENUM_TMPL = """<wsen:Enumerate>
<wsman:EnumerationMode>EnumerateObjectAndEPR</wsman:EnumerationMode>%s
</wsen:Enumerate>"""
PULL_TMPL = """<wsen:Pull>
<wsen:EnumerationContext>%s</wsen:EnumerationContext>
</wsen:Pull>"""
RELEASE_TMPL = """<wsen:Release>
<wsen:EnumerationContext>%s</wsen:EnumerationContext>
</wsen:Release>"""

IDENT_REQ = """<?xml version="1.0" encoding="utf-8" ?>
<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:wsmid="http://schemas.dmtf.org/wbem/wsman/identity/1/wsmanidentity.xsd">
<s:Header/>
<s:Body>
<wsmid:Identify/>
</s:Body>
</s:Envelope>"""
INTR_REQ = """<?xml version="1.0" encoding="utf-8" ?>
<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:wsa="http://schemas.xmlsoap.org/ws/2004/08/addressing" xmlns:wsman="http://schemas.dmtf.org/wbem/wsman/1/wsman.xsd" xmlns:n1="http://schemas.openwsman.org/wbem/wscim/1/intrinsic/%s">
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
XML_REQ = """<?xml version="1.0" encoding="utf-8" ?>
<s:Envelope xmlns:s="http://www.w3.org/2003/05/soap-envelope" xmlns:wsa="http://schemas.xmlsoap.org/ws/2004/08/addressing" xmlns:wsman="http://schemas.dmtf.org/wbem/wsman/1/wsman.xsd" xmlns:wsen="http://schemas.xmlsoap.org/ws/2004/09/enumeration">
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
        elif name == (XML_NS_WS_MAN, "SelectorSet"):
            del self._selectors[:]
        elif self._tag == 'Item': self._tag = name
        elif self._tag: self._pVal = ''
        elif name == (XML_NS_WS_MAN, "Item"): self._tag = 'Item'

    def characters(self, content):
        if content.strip(): self._pVal = str(content)

    def endElementNS(self, name, qname):
#        if name[0] in set((XML_NS_SOAP_1_2, XML_NS_ADDRESSING)): return
        if self._tag == name:
            self._tag = None
        elif name == (XML_NS_WS_MAN, "Selector"):
            sname = self._selectors.pop()
            if sname != '__cimnamespace':
                self._selectors.append('%s="%s"'%(sname, self._pVal))
        elif name == (XML_NS_ENUMERATION, "EnumerationContext"):
            self._cur._enumCtx = self._pVal
        elif name == (XML_NS_WS_MAN, "ResourceURI"):
            self._pdict['__NAMESPACE'],self._pdict['__CLASS'] = \
                                                        self._pVal.rsplit('/',1)
        elif name == (XML_NS_WS_MAN, "SelectorSet"):
            self._pdict['__PATH'] = '%s.%s'%(self._pdict['__CLASS'],
                                            ','.join(self._selectors))
        elif self._tag:
            if self._cur.description: pName = str(name[1]).upper()
            else: pName = str(name[1])
            if '__PATH' in self._pdict:
                self._pVal = self._pdict['__PATH']
            if pName not in self._pdict:
                self._pdict[pName] = self._pVal
            elif type(self._pdict[pName]) is list:
                self._pdict[pName].append(self._pVal)
            else:
                self._pdict[pName] = [self._pdict[pName], self._pVal]
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
        self._connection = connection
        self.description = None
        self.rownumber = -1
        self.arraysize = 1
        self._namespace = connection._namespace
        self._url = connection._url
        self._fltr = connection._fltr
        self._uri = ''
        self._enumCtx = None
        self._props = []
        self._selectors = {}
        self._rows = []
        self._parser = None

    @property
    def rowcount(self):
        """
        Returns number of rows affected by last operation. In case
        of SELECTs it returns meaningful information only after
        all rows has been fetched.
        """
        return len(self._rows)

    def _get_parser(self, intrinsic=False):
        """
        Returns parser object
        """
        if not intrinsic and self._parser:
            return self._parser
        parser = make_parser()
        parser.setFeature(handler.feature_namespaces, 1)
        if intrinsic:
            parser.setContentHandler(intrHandler(self))
        else:
            parser.setContentHandler(WSMHandler(self))
            self._parser = parser
        return parser

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
        self.description = None
        if self._enumCtx:
            self._connection._wsman_request(XML_REQ%(ENUM_ACTION_RELEASE,
                self._url, self._uri, uuid.uuid4(), RELEASE_TMPL%self._enumCtx))
        self._enumCtx = None
        del self._rows[:]
        self._parser = None
        self._selectors.clear()

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
        self._selectors.clear()
        good_sql = False
        if self._enumCtx:
            try: self._connection._wsman_request(XML_REQ%(ENUM_ACTION_RELEASE,
                self._url, self._uri, uuid.uuid4(), RELEASE_TMPL%self._enumCtx))
            finally: self._enumCtx = None

        # for this method default value for params cannot be None,
        # because None is a valid value for format string.

        if (args != () and len(args) != 1):
            raise TypeError("execute takes 1 or 2 arguments (%d given)"%(
                                                                len(args) + 1,))

        if args != ():
            operation = operation%args[0]
        operation = operation.encode('unicode-escape')
        if operation.upper() == 'SELECT 1':
            self._connection._identify()
            self._rows.append((1L,))
            self.description = (('1',CIM_UINT64,None,None,None,None,None),)
            self.rownumber = 0
            return

        try:
            props, classname, where = WQLPAT.match(operation).groups('')
        except:
            raise ProgrammingError("Syntax error in the query statement.")
        if where:
            try:
                self._selectors.update(
                    eval('(lambda **kws:kws)(%s)'%ANDPAT.sub(',', where))
                    )
                if [v for v in self._selectors.values() if type(v) is list]:
                    kbkeys = ''
                    if props != '*':
                        kbkeys = ',%s'%','.join(cursor._selectors.keys())
                    operation = 'SELECT %s%s FROM %s'%(props, kbkeys, classname)
                elif self._connection._fltr: self._selectors.clear()
            except: self._selectors.clear()
        if props == '*': self._props = []
        else: self._props=[p for p in set(props.replace(' ','').split(','))]
        try:
            if not self._connection._wsm_vendor:
                self._connection._identify()
            if 'Microsoft' in self._connection._wsm_vendor:
                classname = '*'
            elif 'Openwsman' in self._connection._wsm_vendor:
                try:
                    self._connection._wsman_request(INTR_REQ%(classname,
                        classname, self._url, classname, uuid.uuid4()),
                        self._get_parser(True))
                except Exception: pass
            if not self._namespace.startswith('http'):
                self._uri = '/'.join(({ 'CIM': XML_NS_CIM_CLASS,
                    'OpenWBEM': XML_NS_OWBEM_CIM_CLASS,
                    'Linux': XML_NS_LINUX_CIM_CLASS,
                    'OMC': XML_NS_OMC_CIM_CLASS,
                    'PG': XML_NS_PG_CIM_CLASS,
                    '*': '/'.join((XML_NS_WIN32_CIM_CLASS, self._namespace)),
                    'Win32': '/'.join((XML_NS_WIN32_CIM_CLASS,self._namespace)),
                    }.get(classname.split('_', 1)[0],
                    XML_NS_CIM_CLASS), classname))
            else: self._uri = '/'.join((self._namespace, classname))
            self._connection._wsman_request(XML_REQ%(ENUM_ACTION_ENUMERATE,
                self._url, self._uri, uuid.uuid4(),
                ENUM_TMPL%(self._fltr and self._fltr%operation or '')),self._get_parser())
            if not self._enumCtx: return
            self._connection._wsman_request(XML_REQ%(ENUM_ACTION_PULL,
                self._url, self._uri, uuid.uuid4(), PULL_TMPL%self._enumCtx),self._get_parser())
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
        try:
            while not self._rows and self._enumCtx:
                self._connection._wsman_request(XML_REQ%(ENUM_ACTION_PULL,
                    self._url, self._uri, uuid.uuid4(),PULL_TMPL%self._enumCtx),self._get_parser())
            if not self._rows: return None
            self.rownumber += 1
            return self._rows.pop(0)
        except OperationalError, e:
            raise OperationalError(e)
        except Exception, e:
            raise OperationalError(e)

    def fetchmany(self, size=None):
        """Fetch up to size rows from the cursor. Result set may be smaller
        than size. If size is not defined, cursor.arraysize is used."""
        self._check_executed()
        if not size: size = self.arraysize
        results = []
        row = self.fetchone()
        while size and row:
            results.append(row)
            size -= 1
            if size: row = self.fetchone()
        return results

    def fetchall(self):
        """Fetchs all available rows from the cursor."""
        self._check_executed()
        results = []
        row = self.fetchone()
        while row:
            results.append(row)
            row = self.fetchone()
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

class wsmanCnx:
    """
    This class represent an WS-Management Connection connection.
    """

    def __init__(self, *args, **kwargs):
        self._connection = None
        self._timeout = float(kwargs.get('timeout', 60))
        self._scheme = str(kwargs.get('scheme', 'https')).lower()
        self._conkwargs = {
            'host':kwargs.get('host') or 'localhost',
            'port':int(kwargs.get('port',self._scheme=='http' and 5985 or 5986))}
        self._path = kwargs.get('path', '/wsman')
        self._url = '%s://%s:%s%s'%(self._scheme, self._conkwargs['host'],
                                    self._conkwargs['port'], self._path)
        self._namespace = kwargs.get('namespace') or 'root/cimv2'
        self._headers = {'Content-type': 'application/soap+xml; charset="utf-8"',
                        'User-Agent': 'pywsmandb'}
        if 'user' in kwargs:
            self._headers['Authorization'] = 'Basic %s'%base64.encodestring(
                '%s:%s'%(kwargs['user'], kwargs.get('password','')))[:-1]
        if self._scheme == 'https':
            if 'key_file' in kwargs and 'cert_file' in kwargs:
                self._conkwargs['key_file'] = kwargs['key_file']
                self._conkwargs['cert_file'] = kwargs['cert_file']
        self._wsm_vendor = ''
        self._fltr={'WQL':WQL_FILTER_TMPL,
                    'CQL':CQL_FILTER_TMPL,
                    }.get(kwargs.get('dialect', '').upper(), '')
        self._lock = threading.Lock()


    def _identify(self):
        """Identify WS-Management ProductVendor 
        """
        try:
            self._wsm_vendor = VENDORPAT.search(
                                    self._wsman_request(IDENT_REQ)).group(1)
        except:
            raise InterfaceError("Access denied")
        if 'Microsoft' in self._wsm_vendor:
            self._fltr = WQL_FILTER_TMPL


    def _wsman_request(self, data, parser=None):
        """Send SOAP+XML data over HTTP to the specified url. Return the
        response in XML.  Uses Python's build-in httplib.
        """

        oldtimeout = None
        try:
            self._lock.acquire()
            headers = {}
            headers.update(self._headers)
            action = ACTIONPAT.search(data)
            if action:
                headers['SOAPAction'] = action.group(1)

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
                    self._connection.request('POST', self._path, data, headers)
                except socket.error, arg:
                    if arg[0] != 104 and arg[0] != 32:
                        raise

                response = self._connection.getresponse()
                xml_resp = response.read()

                if xml_resp.find("'", 0, xml_resp.find("\n")) > 0:
                    xml_resp = xml_resp.replace("'", "", 2)

                if response.status != 200:
                    if response.getheader('CIMError', None) is not None and \
                        response.getheader('PGErrorDetail', None) is not None:
                            import urllib
                            raise InterfaceError("CIMError: (%s, '%s')" %
                                (response.getheader('CIMError'),
                                 urllib.unquote(response.getheader('PGErrorDetail'))))
                    raise InterfaceError('HTTP error: %s'%str((response.status,
                                                            response.reason)))

                if parser:
                    inpsrc = xmlreader.InputSource()
                    inpsrc.setByteStream(StringIO(xml_resp))
                    parser.parse(inpsrc)
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
    timeout       query timeout in seconds
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
