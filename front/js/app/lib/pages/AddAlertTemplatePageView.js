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
 * Add Alert Template Page
 * 
 * This page is where users can add
 * alert templates.
 * 
*/
var Notify = DT.lib.Notifier;
var BasePageView = DT.lib.BasePageView;

// widgets
var AddAlertWidget = require('../widgets/AddAlertWidget');

// class definition
var AddAlertPageView = BasePageView.extend({
    
    pageName: 'AddAlertPageView',
    
    useDashMgr: false,
    
    defaultDashes: [
        {
            dash_id: 'add_alert_dash',
            widgets: [
                { widget: 'addAlertWidget', id: 'Add Alert Template' }
            ]
        }
    ],
    
    initialize: function(options) {
        BasePageView.prototype.initialize.call(this,options);
        
        // Get URL arguments
        var url_args = this.app.nav.get('url_args');
        
        // Create the model
        this.model = new ApplicationModel({
            id: url_args[0]
        },{
            dataSource: this.dataSource
        });
        this.model.setOperators([]);
        this.model.setContainers([]);
        this.model.load(true,true); // first true makes this call synchronous, second true loads logical plan
        this.model.subscribeToUpdates();
        
        // Define the only widget (add alert widget)
        this.defineWidgets([
            { name: 'addAlertWidget', defaultId: 'Add Alert Template', view: AddAlertWidget, limit: 1, inject: {
                instance: this.model,
                dataSource: this.dataSource
            }}
        ]);
        
        this.loadDashboards('add_alert_dash');
    },

    // Extend the cleanup method
    cleanUp: function() {
        this.model.cleanUp();
        delete this.model;
        BasePageView.prototype.cleanUp.call(this);
    }
    
});
exports = module.exports = AddAlertPageView;