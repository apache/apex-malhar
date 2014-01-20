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
 * Ops page
 * 
 * Home page for operations view.
*/
var BasePageView = DT.lib.BasePageView;
var JarFileCollection = DT.lib.JarFileCollection;

// widgets
var JarListWidget = require('../widgets/JarListWidget');
var DepJarListWidget = require('../widgets/DepJarListWidget');

var DevHomePageView = BasePageView.extend({
    
    pageName: 'DevHomePageView',
    
    defaultDashes: [
        {
            dash_id: 'default',
            widgets: [
                { widget: 'JarList', id: 'Application Jars', width: 50 },
                { widget: 'DepJarList', id: 'Dependency Jars', width: 50 }
            ]
        }
    ],
    
    useDashMgr: true,
    
    initialize: function(options) {
        BasePageView.prototype.initialize.call(this,options);
        
        this.defineWidgets([
            {
                name: 'JarList',
                defaultId: 'Uploaded Jars',
                view: JarListWidget,
                limit: 1,
                inject: {
                    nav: this.app.nav
                }
            },
            {
                name: 'DepJarList',
                defaultId: 'Upload Dependency Jars',
                view: DepJarListWidget,
                limit: 1,
                inject: {
                    nav: this.app.nav
                }
            }
        ]);

        this.loadDashboards('default');
    }
    
});

exports = module.exports = DevHomePageView;