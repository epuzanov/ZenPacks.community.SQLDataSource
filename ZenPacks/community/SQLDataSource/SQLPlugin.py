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

$Id: SQLPlugin.py,v 3.0 2012/03/31 0:02:06 egor Exp $"""

__version__ = "$Revision: 3.0 $"[11:-2]

from Products.DataCollector.plugins.CollectorPlugin import CollectorPlugin
from Products.ZenUtils.ZenTales import talesEvalStr
from string import lower
from ZenPacks.community.SQLDataSource.SQLClient import SQLClient

class SQLPlugin(CollectorPlugin):
    """
    A SQLPlugin defines a native Python collection routine and a parsing
    method to turn the returned data structure into a datamap.
    """
    transport = "python"
    tables = {}
    cspropname = "zConnectionString"
    deviceProperties = CollectorPlugin.deviceProperties  +  ('zWinUser',
                                                            'zWinPassword',
                                                            )

    def prepareCS(self, device):
        connectionStrings = getattr(device, self.cspropname, '') or \
            "'pywbemdb',scheme='https',host='${here/manageIp},user='${here/zWinUser}',password='${here/zWinPassword}'"
        extra = {'dev':device}
        if type(connectionStrings) is not list:
            return talesEvalStr(connectionStrings, device, extra)
        return [talesEvalStr(cs, device, extra) for cs in connectionStrings]

    def queries(self, device=None):
        return self.tables

    def prepareQueries(self, device=None):
        return self.queries(device)

    def collect(self, device, log):
        cl = SQLClient(device)
        queries = self.prepareQueries(device)
        return cl.query(queries, sync=False, plugin=self.name())
