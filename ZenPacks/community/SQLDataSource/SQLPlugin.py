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

$Id: SQLPlugin.py,v 3.1 2012/04/05 17:05:20 egor Exp $"""

__version__ = "$Revision: 3.1 $"[11:-2]

from Products.DataCollector.plugins.CollectorPlugin import CollectorPlugin
from Products.ZenUtils.ZenTales import talesEvalStr
from string import lower
from ZenPacks.community.SQLDataSource.SQLClient import SQLClient
from twisted.internet.defer import Deferred

class SQLPlugin(CollectorPlugin):
    """
    A SQLPlugin defines a native Python collection routine and a parsing
    method to turn the returned data structure into a datamap.
    """
    transport = "python"
    tables = {}
    deviceProperties = CollectorPlugin.deviceProperties  +  ('zWinUser',
                                                            'zWinPassword',
                                                            )

    def prepareCS(self, device, connectionStrings=''):
        if device is None:
            return connectionStrings
        def _talesEvalStr(cs, dev, extr):
            newcs = talesEvalStr(cs, device, extr)
            if '${' in newcs:
                newcs = talesEvalStr(newcs, device, extr)
            return newcs
        extra = {'dev':device}
        if type(connectionStrings) is not list:
            return _talesEvalStr(connectionStrings, device, extra)
        return [_talesEvalStr(cs, device, extra) for cs in connectionStrings]

    def queries(self, device=None):
        return self.tables

    def prepareQueries(self, device=None):
        return self.queries(device)

    def clientFinished(self, client):
        for plugin, results in client.getResults():
            if plugin == self: break
        else:
            results = []
        client.deferred.callback(results)
        client = None

    def collect(self, device, log):
        deferred = Deferred()
        cl = SQLClient(device, datacollector=self, plugins=[self])
        setattr(cl, 'deferred', deferred)
        cl.run()
        return deferred