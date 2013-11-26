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
 * Jar App page
 * 
 * Page centered around an application 
 * that exists in an uploaded jar file
*/
var BasePageView = DT.lib.BasePageView;
var JarAppModel = DT.lib.JarAppModel;

// widgets
var JarAppInfoWidget = require('../widgets/JarAppInfoWidget');
var JarAppActionsWidget = require('../widgets/JarAppActionsWidget');
var AppDagWidget = require('../widgets/AppDagWidget');
var JarAppView = BasePageView.extend({
    
    pageName: "JarAppView",
    
    defaultDashes: [
        {
            dash_id: "default",
            widgets: [
                { widget: "JarAppInfo", id: "info", width: 60 },
                { widget: "JarAppActions", id: "actions", width: 40 },
                { widget: "AppDag", id: "logical DAG" }
            ]
        }
    ],
    
    useDashMgr: true,
    
    initialize: function(options) {
        BasePageView.prototype.initialize.call(this,options);
        
        this.model = new JarAppModel({
            name: options.pageParams.appName,
            fileName: options.pageParams.fileName
        });
        this.model.fetch();
        
        this.defineWidgets([
            {
                name: "JarAppInfo",
                defaultId: 'info',
                view: JarAppInfoWidget,
                limit: 1,
                inject: {
                    model: this.model,
                    nav: this.app.nav
                }
            },
            {
                name: "JarAppActions",
                defaultId: 'actions',
                view: JarAppActionsWidget,
                limit: 1,
                inject: {
                    model: this.model,
                    nav: this.app.nav
                }
            },
            { 
                name: 'AppDag', 
                defaultId: 'logical DAG',
                view: AppDagWidget, 
                limit: 0, 
                inject: {
                    dataSource: this.dataSource,
                    model: this.model
                }
            }
        ]);
        this.loadDashboards("default");
    }
    
});

exports = module.exports = JarAppView;