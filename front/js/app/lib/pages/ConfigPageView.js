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
var kt = require('knights-templar');
var BasePageView = DT.lib.BasePageView;
var ConfigPropertyCollection = DT.lib.ConfigPropertyCollection;
var ConfigIssueCollection = DT.lib.ConfigIssueCollection;

// widgets
var ConfigTableWidget = require('../widgets/ConfigTableWidget');

var ConfigPageView = BasePageView.extend({

    pageName: 'ConfigPageView',

    defaultDashes: [
        {
            dash_id: 'default',
            widgets: [
                { widget: 'ConfigTable', id: 'Properties' }
            ]
        }
    ],

    useDashMgr: false,

    initialize: function(options) {
        BasePageView.prototype.initialize.call(this,options);
        
        this.properties = new ConfigPropertyCollection([]);
        this.properties.fetch();
        window.props = this.properties;
        this.issues = new ConfigIssueCollection([]);
        this.issues.fetch();

        this.defineWidgets([
            {
                name: 'ConfigTable',
                defaultId: 'Config Properties',
                view: ConfigTableWidget,
                limit: 1,
                inject: {
                    collection: this.properties,
                    issues: this.issues,
                    dataSource: this.dataSource
                }
            }
        ]);
        this.loadDashboards('default');
    }

});

exports = module.exports = ConfigPageView;