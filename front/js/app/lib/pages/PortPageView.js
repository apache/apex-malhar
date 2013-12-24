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
 * Port View.
 */

var _ = require('underscore');
var Notifier = DT.lib.Notifier;
var BasePageView = DT.lib.BasePageView;
var path = require('path');

// model
var PortModel = DT.lib.PortModel;

// widgets
var PortInfoWidget = require('../widgets/PortInfoWidget');
var PortOverviewWidget = require('../widgets/PortOverviewWidget');
var PortMetricsWidget = require('../widgets/PortMetricsWidget/PortMetricsWidget');

var PortPageView = BasePageView.extend({

    pageName: 'PortPageView',

    useDashMgr: true,

    initialize: function(options) {
        BasePageView.prototype.initialize.call(this, options);

        var pageParams = options.pageParams;

        this.model = new PortModel({
            appId: pageParams.appId,
            operatorId: pageParams.operatorId,
            name: pageParams.portName
        }, {
            dataSource: this.dataSource
        });
        this.model.fetch();
        this.model.subscribe();

        // Define widgets
        this.defineWidgets([
            { name: 'info', defaultId: 'info', view: PortInfoWidget, limit: 0, inject: {
                model: this.model,
                nav: this.app.nav
            }},
            { name: 'overview', defaultId: 'overview', view: PortOverviewWidget, limit: 0, inject: {
                model: this.model,
                nav: this.app.nav
            }},
            { name: 'portmetrics', defaultId: 'metrics', view: PortMetricsWidget, limit: 0, inject: {
                model: this.model,
                nav: this.app.nav
            }}
        ]);

        this.loadDashboards('default');
    },

    defaultDashes: [
        {
            dash_id: 'default',
            widgets: [
                { widget: 'info', id: 'info' },
                { widget: 'overview', id: 'overview'},
                { widget: 'portmetrics', id: 'metrics'}
            ]
        }
    ],

    cleanUp: function() {
        this.model.stopListening();
        BasePageView.prototype.cleanUp.call(this);
    }

});

exports = module.exports = PortPageView