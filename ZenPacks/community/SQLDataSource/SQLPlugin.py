################################################################################
#
# This program is part of the SQLDataSource Zenpack for Zenoss.
# Copyright (C) 2010-2012 Egor Puzanov.
#
# This program can be used under the GNU General Public License version 2
# You can find full information here: http://www.zenoss.com/oss
#
################################################################################

__doc__="""SQLPlugin

wrapper for PythonPlugin

$Id: SQLPlugin.py,v 2.13 2012/03/24 0:04:39 egor Exp $"""

__version__ = "$Revision: 2.13 $"[11:-2]

from Products.DataCollector.plugins.CollectorPlugin import CollectorPlugin
from string import lower
from SQLClient import SQLClient

class SQLPlugin(CollectorPlugin):
    """
    A SQLPlugin defines a native Python collection routine and a parsing
    method to turn the returned data structure into a datamap.
    """
    transport = "python"
    tables = {}
    cspropname = "zConnectionString"
    csProperties = (
        ('host', 'manageIp', 'localhost'),
        ('user', 'zWinUser', ''),
        ('password', 'zWinPassword', ''),
        )
    deviceProperties = CollectorPlugin.deviceProperties  +  ('zWinUser',
                                                            'zWinPassword',
                                                            )

    def prepareCS(self, device):
        args = [getattr(device, self.cspropname, '') or \
                                "'pywbemdb',scheme='https',host='localhost'"]
        kwkeys = map(lower, eval('(lambda *arg,**kws:kws)(%s)'%args[0]).keys())
        for csPropName, dPropName, defVal in self.csProperties:
            if csPropName in kwkeys: continue
            val = getattr(device, dPropName, defVal)
            if isinstance(val, (str, unicode)): val = "'%s'"%val
            args.append("=".join((csPropName, val)))
        return ','.join(args)

    def queries(self, device=None):
        return self.tables

    def prepareQueries(self, device=None):
        return self.queries(device)

    def collect(self, device, log):
        cl = SQLClient(device)
        queries = self.prepareQueries(device)
        return cl.query(queries, sync=False, plugin=self.name())
