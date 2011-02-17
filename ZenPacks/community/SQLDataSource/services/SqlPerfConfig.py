################################################################################
#
# This program is part of the SQLDataSource Zenpack for Zenoss.
# Copyright (C) 2010, 2011 Egor Puzanov.
#
# This program can be used under the GNU General Public License version 2
# You can find full information here: http://www.zenoss.com/oss
#
################################################################################

__doc__="""SqlPerfConfig

Provides config to zenperfsql clients.

$Id: SqlPerfConfig.py,v 1.2 2011/02/17 22:35:58 egor Exp $"""

__version__ = "$Revision: 1.2 $"[11:-2]

from Products.ZenCollector.services.config import CollectorConfigService
from Products.ZenUtils.ZenTales import talesEval
from Products.ZenModel.Device import Device
from ZenPacks.community.SQLDataSource.datasources.SQLDataSource \
    import SQLDataSource as DataSource

import logging
log = logging.getLogger('zen.SqlPerfConfig')


class SqlPerfConfig(CollectorConfigService):

    def _createDeviceProxy(self, device):
        proxy = CollectorConfigService._createDeviceProxy(self, device)

        # for now, every device gets a single configCycleInterval based upon
        # the collector's winCycleInterval configuration which is typically
        # located at dmd.Monitors.Performance._getOb('localhost').
        # TODO: create a zProperty that allows for individual device schedules
        proxy.configCycleInterval = self._prefs.perfsnmpCycleInterval
        queries = {}
        datapoints = {}
        threshs = {}
        log.debug('device: %s', device)
        try: perfServer = device.getPerformanceServer()
        except: return None
        for comp in [device] + device.getMonitoredComponents():
            try: basepath = comp.rrdPath()
            except: continue
            dpnames = {}
            for templ in comp.getRRDTemplates():
                for ds in templ.getRRDDataSources():
                    if not (isinstance(ds, DataSource) and ds.enabled): continue
                    sql, sqlp, kbs, cs = ds.getQueryInfo(comp) or ('','','','')
                    if not sql: continue
                    tn = '/'.join([device.id, comp.id, templ.id, ds.id])
                    if cs not in datapoints: datapoints[cs] = []
                    columns = {}
                    for dp in ds.getRRDDataPoints():
                        dpname = dp.name()
                        dpnames[dpname] = cs
                        alias = dp.id.strip()
                        expr = None
                        for alias in dp.aliases():
                            expr = talesEval("string:%s"%alias.formula, comp,
                                                        extra={'now':'now'})
                            alias = alias.id.strip()
                            break
                        if alias not in columns: columns[alias] = []
                        columns[alias].append(dp.id)
                        datapoints[cs].append((tn, dp.id,
                                isinstance(comp, Device) and "" or comp.id,
                                expr,
                                "/".join((basepath, dpname)),
                                dp.rrdtype,
                                dp.getRRDCreateCommand(perfServer),
                                (dp.rrdmin, dp.rrdmax)))
                    queries[tn] = (sql, sqlp, kbs, cs, columns)
                for thrld in templ.thresholds():
                    if not thrld.enabled: continue
                    for ds in thrld.dsnames:
                        cs = dpnames.get(ds, None)
                        if not cs: continue
                        if cs not in threshs: threshs[cs] = []
                        threshs[cs].append(thrld.createThresholdInstance(comp))
                        break
        proxy.queries = queries
        proxy.datapoints = datapoints
        proxy.thresholds = threshs
        if not queries:
            log.debug("Device %s skipped because there are no datasources",
                          device.getId())
            return None
        return proxy
