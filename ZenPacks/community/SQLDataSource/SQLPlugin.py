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

$Id: SQLPlugin.py,v 2.9 2011/12/17 13:45:07 egor Exp $"""

__version__ = "$Revision: 2.9 $"[11:-2]

from Products.DataCollector.plugins.CollectorPlugin import CollectorPlugin
from twisted.python.failure import Failure
from SQLClient import SQLClient

class SQLPlugin(CollectorPlugin):
    """
    A SQLPlugin defines a native Python collection routine and a parsing
    method to turn the returned data structure into a datamap.
    """
    transport = "python"
    tables = {}
    cspropname = 'zConnectionString'
    deviceProperties = CollectorPlugin.deviceProperties  +  ('zWinUser',
                                                            'zWinPassword',
                                                            )


    def prepareCS(self, device):
        args = [getattr(device, self.cspropname,
                        "'pywbemdb',scheme='https',host='localhost',port=5989")]
        kwargs = eval('(lambda *argsl,**kwargs:kwargs)(%s)'%args[0].lower())
        if 'host' not in kwargs:
            args.append("host='%s'"%getattr(device, 'manageIp', 'localhost'))
        if 'user' not in kwargs:
            args.append("user='%s'"%getattr(device, 'zWinUser', ''))
        if 'password' not in kwargs:
            args.append("password='%s'"%getattr(device, 'zWinPassword', ''))
        return ','.join(args)


    def queries(self, device=None):
        return self.tables


    def prepareQueries(self, device=None):
        return self.queries(device)


    def collect(self, device, log):
        results = {}
        cl = SQLClient(device)
        try: results.update(cl.query(self.prepareQueries(device), True))
        except Exception, ex: pass #log.error("Error: %s", ex)
        cl.close()
        cl = None
        return results


    def preprocess(self, results, log):
        for table in results.keys():
            if isinstance(results[table], Failure): results[table] = []
        return results

