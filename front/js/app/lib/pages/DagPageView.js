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
 * App Instance View
 *
 * Main view for an app instance
 */
var Notify = DT.lib.Notifier;
var BasePageView = DT.lib.BasePageView;
var ApplicationModel = DT.lib.ApplicationModel;

// widgets
var InstanceInfoWidget = require('../widgets/InstanceInfoWidget');
var PhysicalDagWidget = require('../widgets/PhysicalDagWidget');

var DagPageView = BasePageView.extend({

    pageName: 'DagPageView',

    defaultDashes: [
        {
            dash_id: 'default',
            widgets: [
                { widget: 'instanceInfo', id: 'info' },
                { widget: 'appDag', id: 'appDag' }
            ]
        }
    ],

    useDashMgr: true,

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
        this.model.load(true); // true makes this call synchronous
        this.model.subscribeToUpdates();

        this.defineWidgets([
            { name: 'instanceInfo', defaultId: 'info', view: InstanceInfoWidget, inject: {
                model: function() {
                    return this.model;
                }
            }},
            { name: 'appDag', defaultId: 'appDag', view: PhysicalDagWidget, limit: 0, inject: {
                dataSource: this.dataSource,
                model: function() {
                    return this.model
                }
            }}
        ]);

        this.loadDashboards('default');
    },

    // Extend the cleanup method
    cleanUp: function() {
        this.model.cleanUp();
        BasePageView.prototype.cleanUp.call(this);
    }

});

exports = module.exports = DagPageView;