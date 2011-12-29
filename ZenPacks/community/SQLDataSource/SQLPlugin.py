################################################################################
#
# This program is part of the SQLDataSource Zenpack for Zenoss.
# Copyright (C) 2010, 2011 Egor Puzanov.
#
# This program can be used under the GNU General Public License version 2
# You can find full information here: http://www.zenoss.com/oss
#
################################################################################

__doc__="""SQLPlugin

wrapper for PythonPlugin

$Id: SQLPlugin.py,v 2.12 2011/12/29 20:59:30 egor Exp $"""

__version__ = "$Revision: 2.12 $"[11:-2]

from Products.DataCollector.plugins.CollectorPlugin import CollectorPlugin
from twisted.python.failure import Failure
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
        results = {}
        cl = SQLClient(device)
        try: results.update(cl.query(self.prepareQueries(device)))
        except Exception, ex: pass #log.error("Error: %s", ex)
        cl.close()
        cl = None
        return results

    def preprocess(self, results, log):
        for table in results.keys():
            if isinstance(results[table], Failure): results[table] = []
        return results
