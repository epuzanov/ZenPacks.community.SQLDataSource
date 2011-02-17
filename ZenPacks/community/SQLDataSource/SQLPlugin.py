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

$Id: SQLPlugin.py,v 1.1 2011/02/17 22:31:14 egor Exp $"""

__version__ = "$Revision: 1.1 $"[11:-2]

from Products.DataCollector.plugins.CollectorPlugin import CollectorPlugin
from Products.ZenUtils.Driver import drive
from twisted.python.failure import Failure
from twisted.internet import defer
from SQLClient import SQLClient

class SQLPlugin(CollectorPlugin):
    """
    A SQLPlugin defines a native Python collection routine and a parsing
    method to turn the returned data structure into a datamap.
    """
    transport = "python"

    tables = {}

    def queries(self, device = None):
        return self.tables

    def collect(self, device, log):
        def inner(driver):
            results = {}
            for cs, q in queries:
                yield SQLClient(device, cs=cs).query(q, cs)
                results.update(driver.next())
            yield defer.succeed(results)
            driver.next()
        queries = SQLClient().sortQueries(self.queries(device)).iteritems()
        return drive(inner)

    def preprocess(self, results, log):
        newres = {}
        for table, value in results.iteritems():
            if value != []:
                if isinstance(value[0], Failure):
                    log.error(value[0].getErrorMessage())
                    continue
            newres[table] = value
        return newres
