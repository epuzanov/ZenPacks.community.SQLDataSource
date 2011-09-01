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

$Id: SqlPerfConfig.py,v 2.2 2011/09/01 20:05:38 egor Exp $"""

__version__ = "$Revision: 2.2 $"[11:-2]

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
        qIdx = {}
        queries = {}
        log.debug('device: %s', device)
        try: perfServer = device.getPerformanceServer()
        except: return None
        for comp in [device] + device.getMonitoredComponents():
            try: basepath = comp.rrdPath()
            except: continue
            for templ in comp.getRRDTemplates():
                for ds in templ.getRRDDataSources():
                    if not (isinstance(ds, DataSource) and ds.enabled):continue
                    sql, sqlp, kbs, cs = ds.getQueryInfo(comp)
                    if not sql: continue
                    tn = '/'.join([device.id, comp.id, templ.id, ds.id])
                    columns = {}
                    for dp in ds.getRRDDataPoints():
                        dpname = dp.name()
                        alias = (dp.aliases() or [dp])[0]
                        aname = alias.id.strip().upper()
                        if hasattr(alias, 'formula'):
                            expr = talesEval("string:%s"%alias.formula, comp,
                                                        extra={'now':'now'})
                        else: expr = None
                        if aname not in columns: columns[aname] = []
                        columns[aname].append(dp.id)
                        proxy.datapoints.append((cs, tn, dp.id,
                                isinstance(comp, Device) and "" or comp.id,
                                expr,
                                "/".join((basepath, dpname)),
                                dp.rrdtype,
                                dp.getRRDCreateCommand(perfServer),
                                (dp.rrdmin, dp.rrdmax)))

                    table = (tn, columns)
                    ikey = tuple([str(k).upper() for k in (kbs or {}).keys()])
                    ival = tuple([str(v).strip().upper() \
                                                for v in (kbs or {}).values()])
                    if cs not in queries:
                        queries[cs] = {}
                        qIdx[cs] = {}
                    if sqlp not in queries[cs]:
                        if sqlp in qIdx[cs]:
                            queries[cs][sqlp] = qIdx[cs][sqlp][1]
                            del queries[cs][qIdx[cs][sqlp][0]]
                        else:
                            qIdx[cs][sqlp] = (sql, {ikey:{ival:[table]}})
                            queries[cs][sql]={():{():[table]}}
                            continue
                    if ikey not in queries[cs][sqlp]:
                        queries[cs][sqlp][ikey] = {ival:[table]}
                    elif ival not in queries[cs][sqlp][ikey]:
                        queries[cs][sqlp][ikey][ival] = [table]
                    else:
                        queries[cs][sqlp][ikey][ival].append(table)

                dpn = set(templ.getRRDDataPointNames())
                for thr in templ.thresholds():
                    if not (thr.enabled and dpn & set(thr.dsnames)): continue
                    proxy.thresholds.append(thr.createThresholdInstance(comp))

        proxy.queries = queries
        if not queries:
            log.debug("Device %s skipped because there are no datasources",
                          device.getId())
            return None
        return proxy
