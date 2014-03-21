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
var LogicalOperator = DT.lib.LogicalOperatorModel;

// widgets
var LogicalOpInfoWidget     = require('../widgets/LogicalOpInfoWidget');
var LogicalOpOverviewWidget = require('../widgets/LogicalOpOverviewWidget');
var PhysOpListWidget        = require('../widgets/PhysOpListWidget');
var OpMetricsWidget         = require('../widgets/OpMetricsWidget');
var OpPropertiesWidget      = require('../widgets/OpPropertiesWidget');

/**
 * Logical Operator Page Definition
 *
 * This page focuses on one logical operator of a
 * running application.
 */
var LogicalOpPageView = BasePageView.extend({

	pageName: 'LogicalOpPageView',

	useDashMgr: true,

	initialize: function(options){

        var pageParams = options.pageParams;

        BasePageView.prototype.initialize.call(this,options);
        
        // Create logical operator model
        this.model = new LogicalOperator({
            appId: pageParams.appId,
            logicalName: pageParams.logicalName
        }, {
            dataSource: this.dataSource,
            keepPhysicalCollection: true
        });
        this.model.fetch();
        this.model.subscribe();
        
        // Define widgets
        this.defineWidgets([
            {
                name: 'operatorInfo',
                defaultId: 'info',
                view: LogicalOpInfoWidget, 
                inject: {
                    model: this.model
                }
            },
            {
                name: 'operatorOverview',
                defaultId: 'overview',
                view: LogicalOpOverviewWidget,
                inject: {
                    model: this.model
                }
            },
            {
                name: 'partitionList', 
                defaultId: 'partitions',
                view: PhysOpListWidget, 
                limit: 1, 
                inject: {
                    dataSource:this.dataSource, 
                    operators: this.model.physicalOperators, 
                    appId: pageParams.appId, 
                    nav: this.app.nav,
                    noLogicalOperatorLinks: true
                }
            },
            {
                name: 'operatorMetrics',
                defaultId: 'metrics',
                view: OpMetricsWidget,
                inject: {
                    model: this.model
                }
            },
            {
                name: 'operatorProperties',
                defaultId: 'properties',
                view: OpPropertiesWidget,
                inject: {
                    model: this.model,
                    dataSource: this.dataSource
                }
            }
        ]);

        this.loadDashboards('default');
    },

    defaultDashes: [
        {
            dash_id: 'default',
            widgets: [
                { widget: 'operatorInfo', id: 'info', width: 100 },
                { widget: 'operatorOverview', id: 'overview', width: 100 },
                { widget: 'partitionList', id: 'partitions', width: 100 },
                { widget: 'operatorMetrics', id: 'metrics', width: 60 },
                { widget: 'operatorProperties', id: 'properties', width: 40 }
            ]
        }
    ]

});

exports = module.exports = LogicalOpPageView;