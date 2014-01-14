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

var _ = require('underscore');
var Notify = DT.lib.Notifier;
var BasePageView = DT.lib.BasePageView;
var ApplicationModel = DT.lib.ApplicationModel;
var LogicalOperatorCollection = DT.lib.LogicalOperatorCollection;

// widgets
var InstanceInfoWidget = require('../widgets/InstanceInfoWidget');
var InstanceActionWidget = require('../widgets/InstanceActionWidget');
var InstanceOverviewWidget = require('../widgets/InstanceOverviewWidget');
var PhysOpListWidget = require('../widgets/PhysOpListWidget');
var LogicalOpListWidget = require('../widgets/LogicalOpListWidget');
var CtnrListWidget = require('../widgets/CtnrListWidget');
var AppMetricsWidget = require('../widgets/AppMetricsWidget');
var RecListWidget = require('../widgets/RecListWidget');
var StreamListWidget = require('../widgets/StreamListWidget');
var AlertListWidget = require('../widgets/AlertListWidget');
var PhysicalDagWidget = require('../widgets/PhysicalDagWidget');
var LogicalDagWidget = require('../widgets/LogicalDagWidget');
// var TopNWidget = DT.widgets.TopNWidget;


/**
 * App Instance View
 * 
 * Main view for an app instance
*/
var AppInstancePageView = BasePageView.extend({
    
    pageName: 'AppInstancePageView',
    
    useDashMgr: true, 
    
    initialize: function(options) {
        
        BasePageView.prototype.initialize.call(this,options);
        
        // Save options for later use
        this.options = options;

        // Create the model
        this.model = new ApplicationModel(
            { id: options.pageParams.appId },
            { dataSource: this.dataSource }
        );
        
        // Explicitly set operator and container collections
        this.model.setOperators([]);
        this.model.setContainers([]);
        
        // Load the model
        this.model.fetch({
            // Ensures that it loads before the page does.
            // This is required because a running application has a 
            // completely different page than non-running applications.
            async: false 
        });
        this.model.subscribe();
        this.onAppFetch(options.pageParams);
        
        // Listen for state change on model.
        this.listenTo(this.model, 'change:state', function(model, value) {

            // State has changed while looking at this page, issue a message.
            Notify.warning({
                'title': 'Application state changed',
                'text': 'This application\'s state has changed to \''+value+'\'. Please refresh the page to see changes.'
            });
            
        });

        
    },
    
    onAppFetch: function(pageParams) {

        // Define widgets and dashboard group based
        // on whether an app is running or not.
        var defaultDashId, excludeDashId, widgetsToDefine, dashExt, currentState = this.model.get('state');
        
        if (currentState === 'RUNNING' || currentState === 'ACCEPTED') {
            dashExt = ':RUNNING',
            widgetsToDefine = [
                {
                    name: 'instanceInfo', 
                    defaultId: 'info',
                    view: InstanceInfoWidget, 
                    inject: {
                        model: this.model
                    }
                },
                {
                    name: 'instanceAction',
                    defaultId: 'actions',
                    view: InstanceActionWidget, 
                    inject: {
                        model: this.model,
                        dataSource: this.dataSource
                    }
                },
                { 
                    name: 'instanceOverview',
                    defaultId: 'overview',
                    view: InstanceOverviewWidget, 
                    inject: {
                        model: this.model
                    }
                },
                {
                    name: 'physicalOperatorList', 
                    defaultId: 'physical operator list',
                    view: PhysOpListWidget, 
                    limit: 1, 
                    inject: {
                        dataSource:this.dataSource, 
                        operators: this.model.operators,
                        appId: pageParams.appId, 
                        nav: this.app.nav
                    }
                },
                {
                    name: 'logicalOperatorList',
                    defaultId: 'logical operator list',
                    view: LogicalOpListWidget,
                    limit: 1,
                    inject: {
                        pageParams: pageParams,
                        nav: this.app.nav,
                        dataSource: this.dataSource,
                        collection: this.getLogicalOperators.bind(this),
                        onRemove: _.bind(function() {
                            return this.decrementLogicalOperators.bind(this)
                        }, this)
                    }
                },
                {
                    name: 'containerList',
                    defaultId: 'containers list',
                    view: CtnrListWidget,
                    limit: 1,
                    inject: {
                        dataSource: this.dataSource,
                        containers: this.model.containers,
                        instance: this.model,
                        nav: this.app.nav
                    }
                },
                {
                    name: 'appMetrics',
                    defaultId: 'metrics',
                    view: AppMetricsWidget, 
                    inject: { 
                        dataSource:this.dataSource,
                        model: this.model
                    }
                },
                {
                    name: 'appRecordings',
                    defaultId: 'recordings',
                    view: RecListWidget, 
                    inject: {
                        dataSource: this.dataSource,
                        pageParams: pageParams,
                        nav: this.app.nav
                    }
                },
                {
                    name: 'streamList',
                    defaultId: 'stream list',
                    view: StreamListWidget,
                    limit: 1, 
                    inject: {
                        dataSource: this.dataSource,
                        model: this.model,
                        nav: this.app.nav,
                        pageParams: pageParams
                    }
                },
                // {
                //     name: 'operatorChart',
                //     defaultId: 'operator chart',
                //     view: OpChartWidget,
                //     limit: 0, 
                //     inject: {
                //         dataSource: this.dataSource,
                //         model: this.model
                //     }
                // },
                {
                    name: 'alerts',
                    defaultId: 'alert list',
                    view: AlertListWidget,
                    limit: 0,
                    inject: {
                        appId: pageParams.appId
                    }
                },
                {
                    name: 'logicalDAG',
                    defaultId: 'logical DAG',
                    view: LogicalDagWidget,
                    limit: 0,
                        inject: {
                        dataSource: this.dataSource,
                        operators: this.model.operators,
                        model: this.model,
                        collection: this.getLogicalOperators.bind(this),
                        appId: pageParams.appId,
                        onRemove: _.bind(function() {
                            return this.decrementLogicalOperators.bind(this)
                        }, this)
                    }
                },
                {
                    name: 'physicalDAG',
                    defaultId: 'physical DAG',
                    view: PhysicalDagWidget,
                    limit: 0,
                    inject: {
                        dataSource: this.dataSource,
                        model: this.model
                    }
                },
                // {
                //     name: 'topN',
                //     defaultId: 'top n',
                //     view: TopNWidget,
                //     limit: 0, 
                //     inject: {
                //         appId: function() {
                //             return this.model.get('id');
                //         },
                //         dataSource: this.dataSource
                //     }
                // }
            ];
            defaultDashId = 'logical';
            excludeDashIds = ['ended-recordings'];
        } else {
            dashExt = ':STOPPED',
            widgetsToDefine = [
                {
                    name: 'instanceInfo',
                    defaultId: 'defaultId',
                    view: InstanceInfoWidget,
                    inject: {
                        model: this.model
                    }
                },
                {
                    name: 'instanceOverview',
                    defaultId: 'defaultId',
                    view: InstanceOverviewWidget,
                    inject: {
                        model: this.model
                    }
                },
                {
                    name: 'appRecordings',
                    defaultId: 'defaultId',
                    view: RecListWidget,
                    inject: {
                        dataSource: this.dataSource,
                        pageParams: pageParams,
                        table_id: 'ops.app.reclist',
                        nav: this.app.nav
                    }
                }
            ];
            defaultDashId = 'ended-recordings';
            excludeDashIds = ['running-metric-view','operators-containers','alerts-recordings', 'dag-view', 'physical-dag-view'];
        }
        
        // Set the results
        this.defineWidgets(widgetsToDefine);
        this.setLocalKey(dashExt);
        this.loadDashboards(defaultDashId, excludeDashIds);
    },
    
    defaultDashes: [
        {
            dash_id: 'logical',
            widgets: [
                { widget: 'instanceInfo', id: 'info', width: 60 },
                { widget: 'instanceAction', id: 'actions', width: 40 },
                { widget: 'instanceOverview', id: 'overview' },
                { widget: 'logicalDAG', id: 'logical DAG' },
                { widget: 'logicalOperatorList', id: 'logical operators' },
                { widget: 'appMetrics', id: 'metrics', width: 60 },
                { widget: 'streamList', id: 'stream list', width: 40 },
            
            ]
        },
        {
            dash_id: 'physical',
            widgets: [
                { widget: 'instanceInfo', id: 'info', width: 60 },
                { widget: 'instanceAction', id: 'actions', width: 40 },
                { widget: 'instanceOverview', id: 'overview' },
                { widget: 'physicalOperatorList', id: 'physical operators' },
                { widget: 'appMetrics', id: 'metrics', width: 60 },
                { widget: 'containerList', id: 'container list', width: 40 }
            
            ]  
        },
        {
            dash_id: 'physical-dag-view',
            widgets: [
                { widget: 'instanceInfo', id: 'info' },
                { widget: 'physicalDAG', id: 'physical DAG' }
            ]
        },
        {
            dash_id: 'alerts-recordings',
            widgets: [
                { widget: 'instanceInfo', id: 'info', width: 60 },
                { widget: 'instanceAction', id: 'actions', width: 40 },
                { widget: 'instanceOverview', id: 'overview' },
                { widget: 'alerts', id: 'alerts', width: 50 },
                { widget: 'appRecordings', id: 'recording list', width: 50 }
            ]
        },
        {
            dash_id: 'metric-view',
            widgets: [
                { widget: 'instanceInfo', id: 'info', width: 60 },
                { widget: 'instanceAction', id: 'actions', width: 40 },
                { widget: 'instanceOverview', id: 'overview' },
                { widget: 'appMetrics', id: 'metrics chart' }
            ]
        },
        {
            dash_id: 'ended-recordings',
            widgets: [
                { widget: 'instanceInfo', id: 'info' },
                { widget: 'instanceOverview', id: 'overview' },
                { widget: 'appRecordings', id: 'recording list' }
            ]
        }
    ],
    
    cleanUp: function() {
        this.model.cleanUp();
        BasePageView.prototype.cleanUp.call(this);
    },

    logicalOperatorDependencyCount: 0,

    getLogicalOperators: function() {

        ++this.logicalOperatorDependencyCount;

        if (!this.logicalOperators) {
            this.logicalOperators = new LogicalOperatorCollection([], {
                appId: this.options.pageParams.appId,
                dataSource: this.dataSource
            });
        }
        
        var fetchAndSubscribe = _.bind(function() {
            this.logicalOperators.fetch();
            this.logicalOperators.subscribe();
        }, this);

        var currentState = this.model.get('state').toLowerCase();
        var finalStatus = this.model.get('finalStatus').toLowerCase();

        if ( currentState !== 'running' ) {
            
            if ( finalStatus === 'undefined' ) {

                this.listenTo(this.model, 'change:state', function(model, state) {
                    if (state.toLowerCase() === 'running') {
                        fetchAndSubscribe();
                    }
                });
            }
        
        } else {

            fetchAndSubscribe();
        }

        return this.logicalOperators;
    },

    decrementLogicalOperators: function() {
        --this.logicalOperatorDependencyCount;
        if (this.logicalOperatorDependencyCount <= 0) {
            this.logicalOperators.stopListening();
        }
    }
    
});

exports = module.exports = AppInstancePageView;