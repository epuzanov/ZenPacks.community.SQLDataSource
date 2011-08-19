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

$Id: SQLPlugin.py,v 2.2 2011/08/19 19:33:41 egor Exp $"""

__version__ = "$Revision: 2.2 $"[11:-2]

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

    def queries(self, device=None):
        return self.tables


    def prepareQueries(self, device=None):
        return self.queries(device)


    def collect(self, device, log):
        try:
            cl = SQLClient(device)
            results = cl.syncQuery(self.prepareQueries(device))
            cl.close()
            cl = None
            return results
        except:
            log.error("Error opening sql client")

    def preprocess(self, results, log):
        newres = {}
        for table, value in results.iteritems():
            if value != []:
                if isinstance(value[0], Failure):
                    log.error(value[0].getErrorMessage())
                    continue
            newres[table] = value
        return newres

