################################################################################
#
# This program is part of the SQLDataSource Zenpack for Zenoss.
# Copyright (C) 2010-2012 Egor Puzanov.
#
# This program can be used under the GNU General Public License version 2
# You can find full information here: http://www.zenoss.com/oss
#
################################################################################

__doc__="""SQLDataSource

Defines attributes for how a datasource will be graphed
and builds the nessesary DEF and CDEF statements for it.

$Id: SQLDataSource.py,v 2.16 2012/12/05 21:05:28 egor Exp $"""

__version__ = "$Revision: 2.16 $"[11:-2]

from Products.ZenModel.RRDDataSource import RRDDataSource
from Products.ZenModel.ZenPackPersistence import ZenPackPersistence
from Products.ZenUtils.Utils import executeStreamCommand
from Products.ZenWidgets import messaging
from AccessControl import ClassSecurityInfo, Permissions
from twisted.python.failure import Failure

import cgi
import time
import os
import re
import sys

class SQLDataSource(ZenPackPersistence, RRDDataSource):

    ZENPACKID = 'ZenPacks.community.SQLDataSource'

    sourcetypes = ('SQL',)
    sourcetype = 'SQL'
    cs = ''
    sql = ''

    _properties = RRDDataSource._properties + (
        {'id':'cs', 'type':'string', 'mode':'w'},
        {'id':'sql', 'type':'string', 'mode':'w'},
        )

    _relations = RRDDataSource._relations + (
        )

    # Screen action bindings (and tab definitions)
    factory_type_information = ( 
    { 
        'immediate_view' : 'editSQLDataSource',
        'actions'        :
        ( 
            { 'id'            : 'edit'
            , 'name'          : 'Data Source'
            , 'action'        : 'editSQLDataSource'
            , 'permissions'   : ( Permissions.view, )
            },
        )
    },
    )

    security = ClassSecurityInfo()


    def getDescription(self):
        return self.sql


    def useZenCommand(self):
        return False


    def checkCommandPrefix(self, context, cmd):
        """
        Overriding method to verify that zCommandPath is not prepending to our
        Instance name or Query statement.
        """
        return cmd


    def zmanage_editProperties(self, REQUEST=None):
        'add some validation'
        if REQUEST:
            self.cs = REQUEST.get('cs', '')
            self.sql = REQUEST.get('sql', '')
        return RRDDataSource.zmanage_editProperties(self, REQUEST)


    def rePrepare(self, tokens):
        return '[ \\n]%s[ \\n]'%'[ \\n]|[ \\n]'.join(tokens)


    def parseSqlQuery(self, sql):
        skip_tokens = ('LIMIT', 'OR', 'NOT', 'HAVING', 'PROCEDURE','INTO')
        where_end_tokens = ('GROUP BY', 'ORDER BY', 'GO', ';')
        sql_u = sql.upper()
        where_s = sql_u.rfind('WHERE ') + 6
        if where_s < 6: return sql, {}
        if re.search(self.rePrepare(skip_tokens),sql_u[where_s:]):return sql,{}
        where_e = re.search(self.rePrepare(where_end_tokens),sql_u[where_s:])
        if where_e: where_e = where_e.start() + where_s
        else: where_e = len(sql)
        try:
            where = re.compile(' AND ', re.I).sub(',',
                                sql[where_s:where_e].encode('unicode-escape'))
            kbs = eval('(lambda **kws:kws)(%s)'%where)
            FROMPAT = re.compile('[%s]\]?(\s+)FROM\s'%'|'.join(
                                    [(dp.getAliasNames() or [dp.id])[0] \
                                    for dp in self.getRRDDataPoints()]), re.I)
            sql = sql[:where_s - 6] + sql[where_e:]
            newCols=[k for k in kbs.keys() if k.upper() not in sql_u[:where_s]]
            if newCols and '*' not in sql[:where_s]:
                sql = re.sub(FROMPAT, lambda m: m.group(0).replace(m.group(1),
                            ',' + ','.join(kbs.keys()) + m.group(1), 1), sql)
        except: return sql, {}
        return sql.strip(), kbs


    def getConnectionString(self, context):
        connectionString = self.getCommand(context, self.cs)
        if '${' in connectionString:
            connectionString = self.getCommand(context, connectionString)
        return connectionString


    def getQueryInfo(self, context):
        try:
            sql = self.getCommand(context, self.sql)
            try: sqlp, kbs = self.parseSqlQuery(sql)
            except: sqlp, kbs = sql, {}
            return sql, sqlp, kbs, self.getConnectionString(context)
        except: return '', '', {}, ''


    def testDataSourceAgainstDevice(self, testDevice, REQUEST, write, errorLog):
        """
        Does the majority of the logic for testing a datasource against the device
        @param string testDevice The id of the device we are testing
        @param Dict REQUEST the browers request
        @param Function write The output method we are using to stream the result of the command
        @parma Function errorLog The output method we are using to report errors
        """ 
        def writeLines(lines):
            for line in lines.splitlines():
                write(line)

        out = REQUEST.RESPONSE
        # Determine which device to execute against
        device = None
        comp = None
        ttpc = getattr(self.rrdTemplate(), 'targetPythonClass', '')
        ccn = ttpc.rsplit('.', 1)[-1]
        try:
            compClass = getattr(__import__(ttpc,globals(),locals(),[ccn]), ccn)
        except:
            from Products.ZenModel.Device import Device as compClass
        if testDevice:
            # Try to get specified device
            device = self.findDevice(testDevice)
            if not isinstance(device, (compClass, type(None))):
                for comp in device.getMonitoredComponents():
                    if isinstance(comp, compClass): break
                else:
                    comp = None
        elif hasattr(self, 'device'):
            # ds defined on a device, use that device
            device = self.device()
            if not isinstance(device, (compClass, type(None))):
                for comp in device.getMonitoredComponents():
                    if isinstance(comp, compClass): break
                else:
                    comp = None
        elif hasattr(self, 'getSubDevicesGen'):
            # ds defined on a device class, use any device from the class
            for device in self.getSubDevicesGen():
                if isinstance(device, compClass): break
                for comp in device.getMonitoredComponents():
                    if isinstance(comp, compClass): break 
                else:
                    comp = None
                if comp: break
            else:
                device = None
        if not comp:
            comp = device
        if not device:
            errorLog(
                'No Testable Device',
                'Cannot determine a device against which to test.',
                priority=messaging.WARNING
            )
            return self.callZenScreen(REQUEST)
        if not comp:
            errorLog(
                'No component found',
                'Cannot find %s component on device %s.'%(ttpc, device.id),
                priority=messaging.WARNING
            )
            return self.callZenScreen(REQUEST)
        from ZenPacks.community.SQLDataSource.SQLClient import SQLClient
        cl = SQLClient(device)

        header = ''
        footer = ''
        # Render
        if REQUEST.get('renderTemplate', True):
            header, footer = self.commandTestOutput().split('OUTPUT_TOKEN')

        out.write(str(header))

        start = time.time()
        try:
            sql, sqlp, kbs, cs = self.getQueryInfo(comp)
            if not sql:
                raise StandardError('query is empty')
            properties = dict([(dp.id,
                        dp.getAliasNames() and dp.getAliasNames()[0] or dp.id
                        ) for dp in self.getRRDDataPoints()])
            write('Executing query: "%s"'%sql)
            write('')
            write('')
            write('Results:')
            rows = cl.query({'t':(sql, {}, cs, properties)})
            if isinstance(rows, Failure):
                raise StandardError(rows.getErrorMessage())
            rows = rows.get('t') or [{}]
            if isinstance(rows, Failure):
                raise StandardError(rows.getErrorMessage())
            write('|'.join(rows[0].keys()))
            for row in rows:
                write('|'.join(map(str, row.values())))
        except:
            write('exception while executing command')
            write('type: %s  value: %s' % tuple(sys.exc_info()[:2]))
        cl = None
        write('')
        write('')
        write('DONE in %s seconds' % long(time.time() - start))
        out.write(str(footer))


    security.declareProtected('Change Device', 'manage_testDataSource')
    def manage_testDataSource(self, testDevice, REQUEST):
        ''' Test the datasource by executing the command and outputting the
        non-quiet results.
        '''
        # set up the output method for our test
        out = REQUEST.RESPONSE
        def write(lines):
            ''' Output (maybe partial) result text.
            '''
            # Looks like firefox renders progressive output more smoothly
            # if each line is stuck into a table row.  
            startLine = '<tr><td class="tablevalues">'
            endLine = '</td></tr>\n'
            if out:
                if not isinstance(lines, list):
                    lines = [lines]
                for l in lines:
                    if not isinstance(l, str):
                        l = str(l)
                    l = l.strip()
                    l = cgi.escape(l)
                    l = l.replace('\n', endLine + startLine)
                    out.write(startLine + l + endLine)

        # use our input and output to call the testDataSource Method
        errorLog = messaging.IMessageSender(self).sendToBrowser
        return self.testDataSourceAgainstDevice(testDevice,
                                                REQUEST,
                                                write,
                                                errorLog)
