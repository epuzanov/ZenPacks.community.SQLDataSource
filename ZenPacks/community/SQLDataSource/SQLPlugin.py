################################################################################
#
# This program is part of the SQLDataSource Zenpack for Zenoss.
# Copyright (C) 2010-2013 Egor Puzanov.
#
# This program can be used under the GNU General Public License version 2
# You can find full information here: http://www.zenoss.com/oss
#
################################################################################

__doc__="""SQLPlugin

wrapper for PythonPlugin

$Id: SQLPlugin.py,v 3.4 2013/02/27 22:44:04 egor Exp $"""

__version__ = "$Revision: 3.4 $"[11:-2]

from Products.DataCollector.plugins.CollectorPlugin import CollectorPlugin
from Products.ZenUtils.ZenTales import talesEval
from ZenPacks.community.SQLDataSource.SQLClient import SQLClient, getPool
from twisted.internet.defer import Deferred

class SQLPlugin(CollectorPlugin):
    """
    A SQLPlugin defines a native Python collection routine and a parsing
    method to turn the returned data structure into a datamap.
    """
    transport = "python"
    tables = {}
    _pool = getPool('modeler devices')
    deviceProperties = CollectorPlugin.deviceProperties  +  ('zWinUser',
                                                            'zWinPassword',
                                                            )

    def prepareCS(self, device, connectionStrings=''):
        if device is None:
            return connectionStrings
        def _talesEval(cs, dev, extr):
            if not (cs.startswith('string:') or cs.startswith('python:')):
                cs = 'string:%s'%cs
            cs = talesEval(cs, device, extr)
            if '${' in cs:
                cs = talesEval(cs, device, extr)
            return cs
        extra = {'dev':device}
        if type(connectionStrings) is not list:
            return _talesEval(connectionStrings, device, extra)
        return [_talesEval(cs, device, extra) for cs in connectionStrings]

    def queries(self, device=None):
        return self.tables

    def prepareQueries(self, device=None):
        return self.queries(device)

    def clientFinished(self, client):
        results = client.getResults()
        while results:
            plugin, result = results.pop(0)
            plugin.deferred.callback(result)
        poolKey = client.hostname
        if poolKey in self._pool:
            self._pool[poolKey] = None
            del self._pool[poolKey] 

    def collect(self, device, log):
        self.deferred = Deferred()
        cl = self._pool.get(device.id)
        if cl is None:
            cl = SQLClient(device, datacollector=self)
            self._pool[device.id] = cl
        cl.plugins.append(self)
        cl.run()
        return self.deferred
