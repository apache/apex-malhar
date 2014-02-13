/*
* Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
/**
 * Container View
*/

var _ = require('underscore');
var Notifier = DT.lib.Notifier;
var BasePageView = DT.lib.BasePageView;
var ContainerModel = DT.lib.ContainerModel;
var path = require('path');

// widgets
var CtnrInfoWidget = require('../widgets/CtnrInfoWidget');
var CtnrActionWidget = require('../widgets/CtnrActionWidget');
var CtnrOverviewWidget = require('../widgets/CtnrOverviewWidget');
var CtnrMetricsWidget = require('../widgets/CtnrMetricsWidget');
var OpListWidget = require('../widgets/PhysOpListWidget');
var GaugeWidget = require('../widgets/GaugeWidget');
var MemoryGaugeModel = require('../../../datatorrent/MemoryGaugeModel');
var CpuGaugeModel = require('../../../datatorrent/CpuGaugeModel');

var ContainerPageView = BasePageView.extend({
    
    pageName: 'ContainerPageView',
    
    useDashMgr: true,
    
    initialize: function(options){
        BasePageView.prototype.initialize.call(this,options);
        
        // Get initial args
        var pageParams = options.pageParams;
        
        // Instantiate the container
        this.model = new ContainerModel({
            appId: pageParams.appId,
            id: pageParams.containerId
        }, {
            dataSource: this.dataSource
        });
        this.model.setOperators([]);
        this.model.fetch();
        this.model.operators.fetch();
        this.model.subscribe();

        // Define widgets
        this.defineWidgets([
            { name: 'info', defaultId: 'container info', view: CtnrInfoWidget, limit: 0, inject: {
                model: this.model, 
                nav: this.app.nav
            }},
            { name: 'actions', defaultId: 'actions', view: CtnrActionWidget, limit: 0, inject: {
                model: this.model
            }},
            { name: 'overview', defaultId: 'overview', view: CtnrOverviewWidget, limit: 0, inject: {
                model: this.model
            }},
            { name: 'memoryGauge', defaultId: 'memory gauge', view: GaugeWidget, limit: 0, inject: {
                label: 'Memory',
                model: function() { return new MemoryGaugeModel(null, { model: this.model }); }
            }},
            { name: 'cpuGauge', defaultId: 'CPU gauge', view: GaugeWidget, limit: 0, inject: {
                label: 'CPU',
                model: function() { return new CpuGaugeModel(null, { operators: this.model.operators }); }
            }},
            { name: 'operatorList', defaultId: 'operator list', view: OpListWidget, limit: 1, inject: {
                dataSource:this.dataSource,
                operators: this.model.operators, 
                appId: this.model.get('appId'), 
                nav: this.app.nav
            }},
            { name: 'containerMetrics', defaultId: 'metrics', view: CtnrMetricsWidget, limit: 0, inject: {
                dataSource:this.dataSource,
                model: this.model
            }}
        ]);
        
        this.loadDashboards('default');
    },
    
    defaultDashes: [
        {
            dash_id: 'default',
            widgets: [
                { widget: 'info', id: 'container info', width: 65 },
                { widget: 'actions', id: 'actions', width: 35 },
                { widget: 'overview', id: 'overview' },
                { widget: 'memoryGauge', id: 'memory gauge', width: 50 },
                { widget: 'cpuGauge', id: 'CPU gauge', width: 50 },
                { widget: 'operatorList', id: 'operator list'},
                { widget: 'containerMetrics', id: 'metrics'}
            ]
        }
    ],
    
    cleanUp: function() {
        this.model.stopListening();
        this.model.operators.stopListening();
        BasePageView.prototype.cleanUp.call(this);
    }
});

exports = module.exports = ContainerPageView