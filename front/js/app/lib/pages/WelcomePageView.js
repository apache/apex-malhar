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

// widgets
var ConfigWelcomeWidget = require('../widgets/ConfigWelcomeWidget');

var WelcomePageView = BasePageView.extend({

    pageName: 'WelcomePageView',

    defaultDashes: [
        {
            dash_id: 'Welcome!',
            widgets: [
                { widget: 'ConfigWelcome', id: 'Welcome!' }
            ]
        }
    ],

    useDashMgr: false,

    initialize: function(options) {
        BasePageView.prototype.initialize.call(this,options);

        this.defineWidgets([
            {
                name: 'ConfigWelcome',
                defaultId: 'Welcome!',
                view: ConfigWelcomeWidget,
                limit: 1,
                inject: {
                    dataSource: this.dataSource
                }
            }
        ]);
        this.loadDashboards('Welcome!');
    }

});

exports = module.exports = WelcomePageView;