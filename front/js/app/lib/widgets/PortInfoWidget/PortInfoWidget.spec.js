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
var PortInfoWidget = require('./PortInfoWidget');
var Backbone = require('backbone');

describe('PortInfoWidget.js', function() {

    before(function() {
        this.sandbox = sinon.sandbox.create();
        this.sandbox.stub(PortInfoWidget.prototype, 'render');
    });

    beforeEach(function() {
        var mockWidgetDef = new Backbone.Model();
        var mockDashDef = new Backbone.Model({
            widgets: new Backbone.Collection([])
        });
        var mockModel = new Backbone.Model();

        this.view = new PortInfoWidget({
            widget: mockWidgetDef,
            dashboard: mockDashDef,
            model: mockModel
        });
    });

    it('should render() only once on multiple model changes', function() {
        this.view.model.trigger('change');
        this.view.model.trigger('change');
        expect(this.view.render).to.have.been.calledOnce;
    });

    after(function() {
        this.sandbox.restore();
    });

});