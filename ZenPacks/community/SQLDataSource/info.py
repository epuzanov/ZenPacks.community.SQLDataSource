################################################################################
#
# This program is part of the SQLDataSource Zenpack for Zenoss.
# Copyright (C) 2010-2012 Egor Puzanov.
#
# This program can be used under the GNU General Public License version 2
# You can find full information here: http://www.zenoss.com/oss
#
################################################################################

__doc__="""info.py

Representation of Data Source.

$Id: info.py,v 1.2 2012/06/11 21:55:15 egor Exp $"""

__version__ = "$Revision: 1.2 $"[11:-2]

from zope.interface import implements
from Products.Zuul.infos import ProxyProperty
from Products.Zuul.infos.template import RRDDataSourceInfo
from ZenPacks.community.SQLDataSource.interfaces import ISQLDataSourceInfo


class SQLDataSourceInfo(RRDDataSourceInfo):
    implements(ISQLDataSourceInfo)

    @property
    def testable(self):
        """
        We can test this datsource against a specific device
        """
        return True

    cycletime = ProxyProperty('cycletime')
    cs = ProxyProperty('cs')
    sql = ProxyProperty('sql')
