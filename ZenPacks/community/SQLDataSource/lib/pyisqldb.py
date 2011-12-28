#***************************************************************************
# pyisqldb - A DB API v2.0 compatible wrapper to unixODBC isql.
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
__version__ = '1.0.7'

from string import upper, strip
import threading
import datetime
import subprocess
import os
import signal
import re
DTPAT = re.compile(r'^(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})')

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
    return "%04d-%02d-%02d"%args

def Time(*args):
    """
    This function constructs an object holding a time value.
    """
    return "%02d:%02d:%02d"%args

def Timestamp(*args):
    """
    This function constructs an object holding a time stamp value.
    """
    return "%04d-%02d-%02d %02d:%02d:%02d"%args

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

STRING = DBAPITypeObject(0, 3)
BINARY = DBAPITypeObject()
NUMBER = DBAPITypeObject(1)
DATETIME = DBAPITypeObject(2)
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

class isqlCursor(object):
    """
    This class emulate a database cursor, which is used to issue queries
    and fetch results from a unixODBC isql connection.
    """

    def __init__(self, connection):
        """
        Initialize a Cursor object. connection is a wsmanCnx object instance.
        """
        self._args = connection._args
        self._timeout = connection._timeout
        self.rowcount = 0
        self.rownumber = -1
        self.arraysize = 1
        self._description = None
        self._rows = []
        self._queue = []


    @property
    def description(self):
        self._check_executed()
        return self._description

    def _check_executed(self):
        if not self._description and self._queue:
            self._commit(self)
        if not self._description:
            raise OperationalError("No data available. execute() first.")

    def __del__(self):
        self.close()

    def close(self):
        """
        Closes the cursor. The cursor is unusable from this point.
        """
        self._description = None
        del self._rows[:]
        del self._queue[:]

    def _convert(self, value):
        value = str(value).strip()
        if value.isdigit(): return long(value)
        if value.replace('.', '', 1).isdigit(): return float(value)
        if value.lower() == 'false': return False
        if value.lower() == 'true': return True
        r = DTPAT.match(value)
        if not r: return str(value)
        return datetime.datetime(*map(int, r.groups(0)))

    def _commit(self, cursor):
        """
        Commit transaction which is currently in progress.
        """
        if self._queue == []: return
        rows = []
        tCount = 0
        cMap = []
        try:
            p = subprocess.Popen(self._args, bufsize=-1,
                                stdin=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                stdout=subprocess.PIPE)
            queries = [q.strip().replace('\n',' ') for q in self._queue]
            del self._queue[:]
            t = threading.Timer(self._timeout, os.kill, [p.pid, signal.SIGTERM])
            t.start()
            try:
                lines, err = p.communicate('%s\n'%'\n'.join(queries))
            except Exception, e:
                err = t.isAlive() and str(e) or 'Operation timed out'
            t.cancel()
            if err: raise OperationalError(err.strip())
            for line in lines.splitlines():
                if line.startswith('[ISQL]INFO:'): pass
                elif line.strip() == '': pass
                elif line.startswith('+---'):
                    if tCount == 3:
                        tCount = 0
                        del rows[:]
                    if tCount == 0:
                        cMap = map(len, line[2:-2].split('+-'))
                    tCount += 1
                elif line.startswith('| '):
                    props = []
                    cEnd = 0
                    for cLen in cMap:
                        cStart = cEnd + 2
                        cEnd = cStart + cLen
                        if tCount == 2:
                            props.append(self._convert(line[cStart:cEnd]))
                        else:
                            props.append(line[cStart:cEnd])
                    rows.append(tuple(props))
            if len(rows) < 2: return
            descr = []
            for col, value in zip(rows[0], rows[1]):
                cName = col.strip()
                maxlen = None
                cType = 1
                if type(value) == str:
                    if len(col) > len(cName): maxlen = len(col)
                    cType = 0
                elif type(value) == datetime.datetime: cType = 2
                elif type(value) == bool: cType = 3
                descr.append((cName, cType, maxlen, maxlen, None, None, None))
            self._description = tuple(descr)
            self._rows.extend(rows[1:])
            self.rowcount = len(self._rows)
            self.rownumber = 0
        except Exception, e:
            raise OperationalError(e)

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
        if not self._args:
            raise InterfaceError("Connection closed.")
        self._description = None
        del self._rows[:]
        self.rownumber = -1

        # for this method default value for params cannot be None,
        # because None is a valid value for format string.

        if (args != () and len(args) != 1):
            raise TypeError("execute takes 1 or 2 arguments (%d given)" % (
                                                            len(args) + 1,))

        if args != ():
            operation = operation%args[0]
        self._queue.append(operation)

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
        self._check_executed()

    def setoutputsize(self, size=None, column=0):
        """
        This method does nothing, as permitted by DB-API specification.
        """
        self._check_executed()


### connection object

class isqlCnx:
    """
    This class represent an WBEM Connection connection.
    """
    def __init__(self, *args, **kwargs):
        self._timeout = 20
        dsn = None
        uid = None
        pwd = None
        isqlcmd = 'isql'
        kwargs.update(dict(map(strip, i.split('=')) for i in (
            args and args[0] or kwargs.pop('cs', '')).split(';') if '=' in i))
        kwargs['DRIVER'] = kwargs.get('DRIVER', 'None').strip('{}')
        for k in kwargs.keys():
            ku = k.upper()
            if ku in ('UID', 'USER'): uid = kwargs.pop(k)
            elif ku in ('PWD', 'PASSWORD'): pwd = kwargs.pop(k)
            elif ku in ('DSN', 'FILEDSN'): dsn = kwargs.pop(k)
            elif ku in ('SERVER', 'HOST') and kwargs['DRIVER'] == 'PostgreSQL':
                kwargs['servername'] = kwargs.pop(k)
            elif ku == 'ANSI':
                isqlcmd=upper(str(kwargs.pop(k)))=='FALSE' and 'iusql' or 'isql'
            elif ku == 'TIMEOUT': self._timeout = float(kwargs.pop(k))
        if not dsn:
            import md5
            newcs = ';'.join(('%s = %s' %o for o in kwargs.iteritems()))
            dsn = md5.new(newcs).hexdigest()
            f = open(os.path.expanduser('~/.odbc.ini'), 'a+')
            while True:
                line = f.readline()
                if not line:
                    f.write('[%s]\n' % dsn)
                    for param in kwargs.iteritems():
                        f.write('%s = %s\n' % param)
                    break
                if line.startswith('[%s]' % dsn): break
            f.close()
        self._args =  [isqlcmd, dsn, "-c", "-b"]
        if uid:
            self._args.insert(2, uid)
            if pwd: self._args.insert(3, pwd)


    def __del__(self):
        self.close()


    def close(self):
        """
        Close connection to the database. Implicitly rolls back
        all uncommitted transactions.
        """
        pass

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
        return isqlCursor(self)

    def autocommit(self, status):
        """
        Turn autocommit ON or OFF.
        """
        return


# connects to a database server using unixODBC isql
def Connect(*args, **kwargs):

    """
    Constructor for creating a connection to the unixODBC isql. Returns
    a unixODBC isql Connection object. Paremeters are as follows:

    cs            connection string
    uid           user to connect as
    pwd           user's password
    timeout       query timeout in seconds

    Examples:
    con = pyisqldb.connect('DRIVER={MySQL};OPTION=3;PORT=3307;Database=information_schema;SERVER=127.0.0.1',
                            uid='username',
                            pwd='P@ssw0rd')
    """

    try:
        import pyodbc
        return pyodbc.connect(*args, **kwargs)
    except:
        return isqlCnx(*args, **kwargs)

connect = Connection = Connect

__all__ = [ 'BINARY', 'Binary', 'Connect', 'Connection', 'DATE',
    'Date', 'Time', 'Timestamp', 'DateFromTicks', 'TimeFromTicks',
    'TimestampFromTicks', 'DataError', 'DatabaseError', 'Error',
    'FIELD_TYPE', 'IntegrityError', 'InterfaceError', 'InternalError',
    'NULL', 'NUMBER', 'NotSupportedError', 'DBAPITypeObject',
    'OperationalError', 'ProgrammingError', 'ROWID', 'STRING', 'TIME',
    'TIMESTAMP', 'Warning', 'apilevel', 'connect', 'paramstyle','threadsafety']
