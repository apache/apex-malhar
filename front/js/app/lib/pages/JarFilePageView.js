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
var JarAppsWidget = require('../widgets/JarAppsWidget');
var JarActionWidget = require('../widgets/JarActionWidget');
var AppJarFileModel = DT.lib.AppJarFileModel;

/**
 * Jarfile page
 * 
 * Page centered around an uploaded jar file
*/
var JarFileView = BasePageView.extend({
    
    pageName: "JarFileView",
    
    defaultDashes: [
        {
            dash_id: "default",
            widgets: [
                { widget: 'JarAction', id: 'Actions' },
                { widget: "JarApps", id: "Apps in Jar" }
            ]
        }
    ],
    
    useDashMgr: true,
    
    initialize: function(options) {
        BasePageView.prototype.initialize.call(this,options);

        this.model = new AppJarFileModel({
            name: options.pageParams.fileName
        });

        this.defineWidgets([
            {
                name: "JarApps",
                defaultId: 'Apps in Jar',
                view: JarAppsWidget,
                limit: 1,
                inject: {
                    fileName: options.pageParams.fileName,
                    nav: this.app.nav
                }
            },
            {
                name: "JarAction",
                defaultId: 'Actions',
                view: JarActionWidget,
                limit: 0,
                inject: {
                    model: this.model
                }
            },
        ]);
        this.loadDashboards("default");
    }
    
});

exports = module.exports = JarFileView;