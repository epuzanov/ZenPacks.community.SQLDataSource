###########################################################################
#
# This program is part of Zenoss Core, an open source monitoring platform.
# Copyright (C) 2010-2012 Zenoss Inc.
#
# This program is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License version 2 or (at your
# option) any later version as published by the Free Software Foundation.
#
# For complete information please visit: http://www.zenoss.com/oss/
#
###########################################################################

__doc__ = """SqlPerformanceConfig

Provides configuration to zenperfsql clients.

$Id: SqlPerformanceConfig.py,v 3.6 2012/09/16 16:40:40 egor Exp $"""

__version__ = "$Revision: 3.6 $"[11:-2]

import logging
log = logging.getLogger('zen.HubService.SqlPerformanceConfig')
import traceback

import Globals
from ZODB.POSException import ConflictError

from Products.ZenCollector.services.config import CollectorConfigService
from Products.ZenUtils.ZenTales import talesEval
from ZenPacks.community.SQLDataSource.SQLClient import DataSourceConfig,\
                                                        DataPointConfig
from ZenPacks.community.SQLDataSource.datasources.SQLDataSource \
    import SQLDataSource as DataSource
from Products.ZenEvents.ZenEventClasses import Error, Clear


class SqlPerformanceConfig(CollectorConfigService):
    dsType = 'SQL'

    def __init__(self, dmd, instance):
        CollectorConfigService.__init__(self, dmd, instance)
        self.evtOrgNames = dmd.Events.Status.getOrganizerNames()

    def _getDsDatapoints(self, comp, ds, perfServer, dpnames):
        """
        Given a component a data source, gather its data points
        """
        points = []
        if comp == comp.device():
            component_name = ds.getComponent(comp)
        elif callable(getattr(comp, 'name', None)):
            component_name = comp.name()
        else:
            component_name = getattr(comp, 'id', '')
        basepath = comp.rrdPath()
        for dp in ds.getRRDDataPoints():
            dpnames.add(dp.name())
            alias = (dp.aliases() or [dp])[0]
            formula = getattr(alias, 'formula', None)
            dpc = DataPointConfig()
            dpc.id = dp.id
            if formula:
                dpc.expr=talesEval("string:%s"%formula,comp,extra={'now':'now'})
            dpc.alias = alias.id.strip().lower()
            dpc.component = component_name
            dpc.rrdPath = "/".join((basepath, dp.name()))
            dpc.rrdType = dp.rrdtype
            dpc.rrdCreateSql = dp.getRRDCreateCommand(perfServer)
            dpc.rrdMin = dp.rrdmin
            dpc.rrdMax = dp.rrdmax
            points.append(dpc)
        return points

    def _getDsCycleTime(self, comp, templ, ds):
        cycleTime = 300
        try:
            cycleTime = int(ds.cycletime)
        except ValueError:
            message = "Unable to convert the cycle time '%s' to an " \
                          "integer for %s/%s on %s" \
                          " -- setting to 300 seconds" % (
                          ds.cycletime, templ.id, ds.id, comp.device().id)
            log.error(message)
            component = ds.getPrimaryUrlPath()
            dedupid = "Unable to convert cycletime for %s" % component
            self.sendEvent(dict(
                    device=comp.device().id, component=component,
                    eventClass='/zenperfsql', severity=Warning, summary=message,
                    dedupid=dedupid,  
            ))
        return cycleTime

    def _safeGetComponentConfig(self, comp, device, perfServer,
                                datasources, thresholds):
        """
        Catchall wrapper for things not caught at previous levels
        """
        if not getattr(comp, 'monitorDevice', lambda:None)():
            return None

        try:
            threshs=self._getComponentConfig(comp,device,perfServer,datasources)
            if threshs:
                thresholds.extend(threshs)
        except ConflictError: raise
        except Exception, ex:
            msg = "Unable to process %s datasource(s) for device %s -- skipping" % (
                              self.dsType, device.id)
            log.exception(msg)
            details = dict(traceback=traceback.format_exc(),
                           msg=msg)
            self._sendQueryEvent(device.id, details)

    def _getComponentConfig(self, comp, device, perfServer, queries):
        thresholds = []
        if comp.__class__.__name__ in self.evtOrgNames:
            eventClass = comp.__class__.__name__
        elif comp.meta_type in self.evtOrgNames:
            eventClass = comp.meta_type
        else:
            eventClass = 'PyDBAPI'
        for templ in comp.getRRDTemplates():
            dpnames = set()
            for ds in templ.getRRDDataSources():
                if not (isinstance(ds, DataSource) and ds.enabled): continue
                query = DataSourceConfig()
                query.name = "%s/%s" % (templ.id, ds.id)
                query.cycleTime = self._getDsCycleTime(comp, templ, ds)
                query.component = comp.id
                query.eventKey = ds.eventKey or ds.id
                query.severity = ds.severity
                query.ds = ds.titleOrId()
                query.points = self._getDsDatapoints(comp,ds,perfServer,dpnames)
                if ds.eventClass:
                    query.eventClass = ds.eventClass
                else:
                    query.eventClass = '/Status/%s'%eventClass
                try:
                    query.sql, query.sqlp, query.keybindings, \
                        query.connectionString = ds.getQueryInfo(comp)
                    if not query.sql: continue
                    if query.sqlp and '_process where' in query.sql.lower():
                        query.sql = query.sqlp
                except ConflictError: raise
                except Exception: # TALES error
                    msg = "TALES error for device %s datasource %s" % (
                               device.id, ds.id)
                    details = dict(
                           msg=msg,
                           template=templ.id,
                           datasource=ds.id,
                           affected_device=device.id,
                           affected_component=comp.id,
                           resolution='Could not create a connection string or SQL query ' \
                                      ' to send to zenperfsql because TALES evaluation' \
                                      ' failed.  The most likely cause is unescaped ' \
                                      ' special characters in the sonnection string or' \
                                      ' SQL query. eg $ or %')
                    # This error might occur many, many times
                    self._sendQueryEvent('localhost', details)
                    continue

                self.enrich(query, templ, ds)
                queries.add(query)

            for threshold in templ.thresholds():
                if threshold.enabled and dpnames & set(threshold.dsnames):
                    thresholds.append(threshold.createThresholdInstance(comp))

        return thresholds

    def enrich(self, query, template, ds):
        """
        Hook routine available for subclassed services
        """
        pass

    def _createDeviceProxy(self, device):
        proxy = CollectorConfigService._createDeviceProxy(self, device)

        proxy.configCycleInterval = self._prefs.perfsnmpCycleInterval
        proxy.name = device.id
        proxy.device = device.id
        proxy.lastmodeltime = device.getLastChangeString()
        proxy.lastChangeTime = float(device.getLastChange())

        perfServer = device.getPerformanceServer()
        datasources = set()

        # First for the device....
        proxy.thresholds = []
        self._safeGetComponentConfig(device, device, perfServer,
                                datasources, proxy.thresholds)

        # And now for its components
        for comp in device.getMonitoredComponents():
            self._safeGetComponentConfig(comp, device, perfServer,
                                datasources, proxy.thresholds)

        if datasources:
            proxy.datasources = list(datasources)
            return proxy
        return None

    def _sendQueryEvent(self, name, details=None):
        msg = 'Connection String is not set so SQL query will not run'
        ev = dict(
                device=name,
                eventClass='/Status/PyDBAPI',
                eventKey='zenperfsql',
                severity=Error,
                component='zenperfsql',
                summary=msg,
        )
        if details:
            ev.update(details)
        self.sendEvent(ev)


if __name__ == '__main__':
    try:
        from Products.ZenHub.ServiceTester import ServiceTester
    except:
        import sys
        sys.exit(0)
    tester = ServiceTester(SqlPerformanceConfig)
    def printer(proxy):
        print '\t'.join([ '', 'Name', 'ConnectionString', 'Query',
                    'CycleTime', 'Component', 'Points'])
        for query in sorted(proxy.datasources):
            print '\t'.join( map(str, [ '', query.name, query.connectionString,
                query.sql, query.cycleTime, query.component, query.points ]) )
    tester.printDeviceProxy = printer
    tester.showDeviceInfo()
