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
var PerfMetricsWidget = require('./PerfMetricsWidget');
var Backbone = require('backbone');

describe('PerfMetricsWidget.js', function() {

    it('should override defaults', function() {
        var mockWidgetDef = new Backbone.Model({
            limit: 155
        });
        var mockDashDef = new Backbone.Model({
            widgets: new Backbone.Collection([])
        });
        var mockModel = new Backbone.Model();

        this.view = new PerfMetricsWidget({
            dashboard: mockDashDef,
            widget: mockWidgetDef,
            model: mockModel
        });
        var limit = this.view.state.get('limit');
        expect(limit).to.equal(155);
    });

});