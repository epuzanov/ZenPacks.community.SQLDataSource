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

$Id: info.py,v 1.1 2012/03/28 23:05:51 egor Exp $"""

__version__ = "$Revision: 1.1 $"[11:-2]

from zope.interface import implements
from Products.Zuul.infos import ProxyProperty
from Products.Zuul.infos.template import InfoBase
from ZenPacks.community.SQLDataSource.interfaces import ISQLDataSourceInfo


class SQLDataSourceInfo(InfoBase):
    implements(ISQLDataSourceInfo)

    def __init__(self, dataSource):
        self._object = dataSource

    @property
    def id(self):
        return '/'.join(self._object.getPrimaryPath())

    @property
    def source(self):
        return self._object.getDescription()

    @property
    def type(self):
        return self._object.sourcetype

    @property
    def testable(self):
        """
        We can NOT test this datsource against a specific device
        """
        return True

    # severity
    def _setSeverity(self, value):
        try:
            if isinstance(value, str):
                value = severityId(value)
        except ValueError:
            # they entered junk somehow (default to info if invalid)
            value = severityId('info')
        self._object.severity = value

    def _getSeverity(self):
        return self._object.getSeverityString()

    enabled = ProxyProperty('enabled')
    cs = ProxyProperty('cs')
    sql = ProxyProperty('sql')
    severity = property(_getSeverity, _setSeverity)
    cycletime = ProxyProperty('cycletime')
    eventKey = ProxyProperty('eventKey')
    eventClass = ProxyProperty('eventClass')
