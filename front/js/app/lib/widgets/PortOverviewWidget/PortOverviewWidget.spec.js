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
var PortOverviewWidget = require('./PortOverviewWidget');
var Backbone = require('backbone');

describe('PortOverviewWidget.js', function() {

    before(function() {
        this.sandbox = sinon.sandbox.create();
        this.sandbox.stub(PortOverviewWidget.prototype, 'renderContent');
    });

    beforeEach(function() {
        var mockWidgetDef = new Backbone.Model({
            id: "mockWidgetDef",
            widget: "mockWidgetDef"
        });
        var mockDashDef = new Backbone.Model({
            widgets: new Backbone.Collection([])
        });
        var mockModel = new Backbone.Model({
            id: "",
            appId: undefined,
            operatorId: undefined,
            bufferServerBytesPSMA10: "",
            name: "",
            totalTuples: "",
            tuplesPSMA10: "",
            type: "",
            selected: true
        });

        this.view = new PortOverviewWidget({
            widget: mockWidgetDef,
            dashboard: mockDashDef,
            model: mockModel
        });
    });

    it('should renderContent() on every model change', function() {
        this.view.model.trigger('change');
        this.view.model.trigger('change');
        this.view.model.trigger('change');
        expect(this.view.renderContent).to.have.been.calledThrice;
    });

    after(function() {
        this.sandbox.restore();
    });

});