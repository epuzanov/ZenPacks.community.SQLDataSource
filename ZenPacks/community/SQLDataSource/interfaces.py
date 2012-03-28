################################################################################
#
# This program is part of the SQLDataSource Zenpack for Zenoss.
# Copyright (C) 2010-2012 Egor Puzanov.
#
# This program can be used under the GNU General Public License version 2
# You can find full information here: http://www.zenoss.com/oss
#
################################################################################

__doc__="""interfaces

describes the form field to the user interface.

$Id: interfaces.py,v 1.2 2012/03/28 23:06:20 egor Exp $"""

__version__ = "$Revision: 1.2 $"[11:-2]

from Products.Zuul.interfaces import IInfo
from Products.Zuul.form import schema
from Products.Zuul.utils import ZuulMessageFactory as _t


class ISQLDataSourceInfo(IInfo):
    name = schema.Text(title=_t(u'Name'))
    enabled = schema.Bool(title=_t(u'Enabled'))
    cs = schema.Text(title=_t(u'Connection String'))
    sql = schema.TextLine(title=_t(u'SQL Query'))
    severity = schema.Text(title=_t(u'Severity'), xtype='severity')
    eventKey = schema.Text(title=_t(u'Event Key'))
    eventClass = schema.Text(title=_t(u'Event Class'), xtype='eventclass')
    cycletime = schema.Int(title=_t(u'Cycle Time (seconds)'))

