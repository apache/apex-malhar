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
 * Physical Operator View
*/

var _ = require('underscore');
var Notifier = DT.lib.Notifier;
var BasePageView = DT.lib.BasePageView;
var Operator = DT.lib.OperatorModel;
var path = require('path');

// widgets
var PhysOpOverviewWidget = require('../widgets/PhysOpOverviewWidget');
var PhysOpInfoWidget = require('../widgets/PhysOpInfoWidget');
var OpMetricsWidget = require('../widgets/OpMetricsWidget');
var PortListWidget = require('../widgets/PortListWidget');
var RecListWidget = require('../widgets/RecListWidget');
var OpActionWidget = require('../widgets/OpActionWidget');
var OpPropertiesWidget = require('../widgets/OpPropertiesWidget');

// page definition
var PhysOpPageView = BasePageView.extend({
    
    pageName: 'PhysOpPageView',
    
    useDashMgr: true,
    
    initialize: function(options){
        BasePageView.prototype.initialize.call(this,options);
        
        // Create operator model
        this.model = new Operator({
            appId: options.pageParams.appId,
            id: options.pageParams.operatorId
        }, {
            dataSource: this.dataSource
        });
        this.model.fetch();
        this.model.subscribe();
        
        // Define widgets
        this.defineWidgets([
            {
                name: 'info',
                defaultId: 'operator info',
                view: PhysOpInfoWidget,
                limit: 0, inject: {
                    model: function() {
                        return this.model
                    },
                    nav: this.app.nav
                }
            },
            {
                name: 'action',
                defaultId: 'action',
                view: OpActionWidget,
                limit: 1, inject: {
                    model: function() {
                        return this.model;
                    },
                    nav: this.app.nav,
                    dataSource: this.dataSource
                }
            },
            {
                name: 'overview',
                defaultId: 'overview',
                view: PhysOpOverviewWidget,
                limit: 0, inject: {
                    model: function() {
                        return this.model
                    }, 
                    nav: this.app.nav
                }
            },

            {
                name: 'opmetrics',
                defaultId: 'metrics', 
                view: OpMetricsWidget,
                limit: 0, inject: {
                    model: function() {
                        return this.model
                    },
                    dataSource: this.dataSource
                }
            },

            {
                name: 'portlist',
                defaultId: 'port list',
                view: PortListWidget,
                limit: 1, inject: {
                    model: function() {
                        return this.model
                    },
                    dataSource: this.dataSource,
                    nav: this.app.nav
                }
            },
            {
                name: 'recordings',
                defaultId: 'recording list',
                view: RecListWidget,
                limit: 0, inject: {
                    dataSource: this.dataSource,
                    pageParams: options.pageParams,
                    table_id: 'ops.app.op.reclist',
                    nav: this.app.nav
                }
            },
            {
                name: 'properties',
                defaultId: 'property list',
                view: OpPropertiesWidget,
                limit: 0, inject: {
                    model: this.model,
                    dataSource: this.dataSource
                }
            }
        ]);
        
        this.loadDashboards('info');
        
    },
    
    defaultDashes: [
        {
            dash_id: 'info',
            widgets: [
                { widget: 'info', id: 'operator info', width: 60 },
                { widget: 'action', id: 'actions', width: 40 },
                { widget: 'overview', id: 'overview'},
                { widget: 'portlist', id: 'portlist'},
                { widget: 'properties', id: 'properties', width: 50},
                { widget: 'recordings', id: 'recordings', width: 50}
            ]
        },
        {
            dash_id: 'metrics',
            widgets: [
                { widget: 'info', id: 'operator info', width: 60 },
                { widget: 'action', id: 'actions', width: 40 },
                { widget: 'overview', id: 'overview'},
                { widget: 'opmetrics', id: 'metrics'}
            ]
        }
    ],
    
    cleanUp: function() {
        this.model.stopListening();
        BasePageView.prototype.cleanUp.call(this);
    }
    
});

exports = module.exports = PhysOpPageView;