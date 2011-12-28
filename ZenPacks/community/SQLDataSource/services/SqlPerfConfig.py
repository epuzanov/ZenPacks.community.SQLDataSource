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

$Id: SqlPerfConfig.py,v 2.10 2011/12/26 20:58:48 egor Exp $"""

__version__ = "$Revision: 2.10 $"[11:-2]

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
        proxy.datapoints = []
        proxy.thresholds = []
        queries = {}
        log.debug('device: %s', device)
        try: perfServer = device.getPerformanceServer()
        except: return None
        evtOrgNames = self.dmd.Events.Status.getOrganizerNames()
        for comp in [device] + device.getMonitoredComponents():
            compName = ""
            compType = ""
            if not isinstance(comp, Device):
                compName = comp.id
                if comp.__class__.__name__ in evtOrgNames:
                    compType = comp.__class__.__name__
                elif comp.meta_type in evtOrgNames:
                    compType = comp.meta_type
            try: basepath = comp.rrdPath()
            except: continue
            for templ in comp.getRRDTemplates():
                dpnames = []
                for ds in templ.getRRDDataSources():
                    if not (isinstance(ds, DataSource) and ds.enabled):continue
                    sql, sqlp, kbs, cs = ds.getQueryInfo(comp)
                    if not sql: continue
                    if sqlp and '_process where' in sql.lower(): sql = sqlp
                    tn = '/'.join([device.id, comp.id, templ.id, ds.id])
                    aliases = set()
                    sortkey = (cs,)
                    for dp in ds.getRRDDataPoints():
                        dpname = dp.name()
                        dpnames.append(dpname)
                        alias = (dp.aliases() or [dp])[0]
                        aname = alias.id.strip().lower()
                        formula = getattr(alias, 'formula', None)
                        expr = formula and talesEval("string:%s"%alias.formula,
                                            comp, extra={'now':'now'}) or None
                        aliases.add(aname)
                        proxy.datapoints.append((sortkey, tn, dp.id, aname,
                                (compName, compType),
                                expr,
                                "/".join((basepath, dpname)),
                                dp.rrdtype,
                                dp.getRRDCreateCommand(perfServer),
                                (dp.rrdmin, dp.rrdmax)))
                    queries.setdefault(sortkey, {})[tn] = (sqlp, kbs, cs,
                                                dict(zip(aliases,aliases)), sql)

                dpn = set(dpnames)
                for thr in templ.thresholds():
                    if not (thr.enabled and dpn & set(thr.dsnames)): continue
                    proxy.thresholds.append(thr.createThresholdInstance(comp))

        proxy.queries = queries
        if not queries:
            log.debug("Device %s skipped because there are no datasources",
                          device.getId())
            return None
        return proxy
