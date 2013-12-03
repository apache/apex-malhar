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

var BasePageView = DT.lib.BasePageView;
var ApplicationCollection = DT.lib.ApplicationCollection;

// widgets
var AppListWidget = require('../widgets/AppListWidget');
var ClusterOverviewWidget = require('../widgets/ClusterOverviewWidget');


/**
 * Home page for operations view.
 * 
 */
var OpsHomePageView = BasePageView.extend({
    
    pageName: 'OpsHomePageView',
    
    defaultDashes: [
        {
            dash_id: 'default',
            widgets: [
                { widget: 'ClusterOverview', id: 'Cluster Overview' },
                { widget: 'AppList', id: 'Application List' }
            ]
        }
    ],
    
    useDashMgr: true,
    
    initialize: function(options) {
        BasePageView.prototype.initialize.call(this,options);
        
        // Set a list of apps on this page
        this.applications = new ApplicationCollection([], {
            dataSource: this.dataSource
        });
        
        // Fetch the entire app list
        this.applications.fetch();
        this.applications.subscribe();
        
        this.defineWidgets([
            {
                name: 'AppList',
                defaultId: 'Application List',
                view: AppListWidget,
                limit: 1,
                inject: {
                    dataSource: this.dataSource,
                    nav: this.app.nav,
                    apps: this.applications
                }
            },
            {
                name: 'ClusterOverview',
                defaultId: 'Cluster Overview',
                view: ClusterOverviewWidget,
                limit: 1,
                inject: {
                    dataSource: this.dataSource
                }
            }
        ]);
        this.loadDashboards('default');
    },
    
    cleanUp: function() {
        this.applications.stopListening();
        BasePageView.prototype.cleanUp.call(this);
    }
    
});

exports = module.exports = OpsHomePageView;