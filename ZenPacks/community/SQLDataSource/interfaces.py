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

$Id: interfaces.py,v 1.3 2012/06/11 21:57:39 egor Exp $"""

__version__ = "$Revision: 1.3 $"[11:-2]

from Products.Zuul.interfaces.template import IRRDDataSourceInfo
from Products.Zuul.form import schema
from Products.Zuul.utils import ZuulMessageFactory as _t


class ISQLDataSourceInfo(IRRDDataSourceInfo):
    """
    Adapts SQLDataSource
    """
    cycletime = schema.Int(title=_t(u'Cycle Time (seconds)'))
    cs = schema.Text(title=_t(u'Connection String'))
    sql = schema.TextLine(title=_t(u'SQL Query'))
