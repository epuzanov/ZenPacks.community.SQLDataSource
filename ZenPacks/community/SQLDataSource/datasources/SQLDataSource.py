################################################################################
#
# This program is part of the SQLDataSource Zenpack for Zenoss.
# Copyright (C) 2010, 2011 Egor Puzanov.
#
# This program can be used under the GNU General Public License version 2
# You can find full information here: http://www.zenoss.com/oss
#
################################################################################

__doc__="""SQLDataSource

Defines attributes for how a datasource will be graphed
and builds the nessesary DEF and CDEF statements for it.

$Id: SQLDataSource.py,v 1.11 2011/03/24 16:51:39 egor Exp $"""

__version__ = "$Revision: 1.11 $"[11:-2]

from Products.ZenModel.RRDDataSource import RRDDataSource
from Products.ZenModel.ZenPackPersistence import ZenPackPersistence
from Products.ZenUtils.Utils import executeStreamCommand
from Products.ZenWidgets import messaging
from AccessControl import ClassSecurityInfo, Permissions

import cgi
import time
import os
import re

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
            where = re.compile(' AND ', re.I).sub(',', sql[where_s:where_e])
            kbs = eval('(lambda **kwargs: kwargs)(%s)'%where)
            FROMPAT = re.compile('[%s]\]?(\s+)FROM\s'%'|'.join(
                                    [(dp.getAliasNames() or [dp.id])[0] \
                                    for dp in self.getRRDDataPoints()]), re.I)
            sql = sql[:where_s - 6] + sql[where_e:]
            newCols=[k for k in kbs.keys() if k.upper() not in sql_u[:where_s]]
            if newCols and '*' not in sql[:where_s]:
                sql = re.sub(FROMPAT, lambda m: m.group(0).replace(m.group(1),
                            ',' + ','.join(kbs.keys()) + m.group(1), 1), sql)
        except: return sql, {}
        return sql, kbs


    def getConnectionString(self, context):
        return self.getCommand(context, self.cs)


    def getQueryInfo(self, context):
        try:
            sql = self.getCommand(context, self.sql)
            sqlp, kbs = self.parseSqlQuery(sql)
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
        if testDevice:
            # Try to get specified device
            device = self.findDevice(testDevice)
            if not device:
                errorLog(
                    'No device found',
                    'Cannot find device matching %s.' % testDevice,
                    priority=messaging.WARNING
                )
                return self.callZenScreen(REQUEST)
        elif hasattr(self, 'device'):
            # ds defined on a device, use that device
            device = self.device()
        elif hasattr(self, 'getSubDevicesGen'):
            # ds defined on a device class, use any device from the class
            try:
                device = self.getSubDevicesGen().next()
            except StopIteration:
                # No devices in this class, bail out
                pass
        if not device:
            errorLog(
                'No Testable Device',
                'Cannot determine a device against which to test.',
                priority=messaging.WARNING
            )
            return self.callZenScreen(REQUEST)
        ttpc = getattr(self.rrdTemplate(), 'targetPythonClass', '')
        try:
            ccm, ccn = ttpc.rsplit('.', 1)
            compClass = getattr(__import__(ttpc,globals(),locals(),[ccn]),ccn)
            if compClass:
                for comp in device.getMonitoredComponents():
                    if isinstance(comp, compClass):
                        device = comp
                        break
        except: pass
        header = ''
        footer = ''
        # Render
        if REQUEST.get('renderTemplate', True):
            header, footer = self.commandTestOutput().split('OUTPUT_TOKEN')

        out.write(str(header))

        start = time.time()
        try:
            import sys
            sql, sqlp, kbs, cs = self.getQueryInfo(device)
            if not sql:
                raise StandardError('query is empty')
            sql = sql.replace('$','\\$')
            properties = dict([(
                        dp.getAliasNames() and dp.getAliasNames()[0] or dp.id,
                        dp.id) for dp in self.getRRDDataPoints()])
            write('Executing query: "%s"'%sql)
            write('')
            zp = self.dmd.ZenPackManager.packs._getOb(
                                    'ZenPacks.community.SQLDataSource', None)
            sql = sql.replace('"', '\\"')
            command = "env PYTHONPATH=\"%s\" python %s -c \"%s\" -q \"%s\" -f \"%s\" -a \"%s\""%(
                                                os.pathsep.join(sys.path),
                                                zp.path('SQLClient.py'),cs,sql,
                                                " ".join(properties.keys()),
                                                " ".join(properties.values()))
            executeStreamCommand(command, writeLines)
        except:
            import sys
            write('exception while executing command')
            write('type: %s  value: %s' % tuple(sys.exc_info()[:2]))
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
