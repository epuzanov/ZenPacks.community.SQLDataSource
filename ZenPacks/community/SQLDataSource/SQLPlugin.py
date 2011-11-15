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

$Id: SQLPlugin.py,v 2.6 2011/11/14 21:50:48 egor Exp $"""

__version__ = "$Revision: 2.6 $"[11:-2]

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
            results = cl.query(self.prepareQueries(device), True)
            cl.close()
            cl = None
            return results
        except Exception, ex:
            log.error("Error: %s", ex)

    def preprocess(self, results, log):
        for table in results.keys():
            if results[table] and isinstance(results[table][0], Failure):
                results[table] = []
        return results

