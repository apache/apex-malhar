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
var Backbone = require('backbone');
var DataSource = DT.lib.DataSource;
var PortPageView = require('./PortPageView');
var PortInfoWidget = require('./../widgets/PortInfoWidget/PortInfoWidget');
var PortOverviewWidget = require('./../widgets/PortOverviewWidget/PortOverviewWidget');

describe('PortPageView.js', function() {

    describe('using stubs', function() {
        before(function() {
            _.each(['sortable', 'tooltip', 'disableSelection'], function(k) {
                $.fn[k] = function() { return $(this) };
            });
            this.dataSource = new DataSource('host', new Backbone.Model());
            this.stubApp = { dataSource: this.dataSource };
            this.stubApp.nav = new Backbone.Model({
                url_args: ['appId', 'operatorId', 'port name']
            });
        });

        beforeEach(function() {
            this.sandbox = sinon.sandbox.create();
            this.sandbox.stub(this.dataSource, 'subscribe');
            this.sandbox.stub(PortInfoWidget.prototype, 'render').returns('');
            this.sandbox.stub(PortOverviewWidget.prototype, 'render').returns('');
            this.sandbox.stub(PortInfoWidget.prototype, 'renderContent').returns('');
            this.sandbox.stub(PortOverviewWidget.prototype, 'renderContent').returns('');

            this.portView = new PortPageView({
                app: this.stubApp,
                pageParams: { appId: 'appId', operatorId: 'operatorId', portName: 'port name' }
            });
        });

        afterEach(function() {
            this.sandbox.restore();
        });

        it('should get data from DataSource', function() {
            expect(this.dataSource.subscribe).to.have.been.calledOnce;
        });

        it('should render default widgets', function() {
            this.portView.render();

            expect(PortInfoWidget.prototype.render).to.have.been.calledOnce;
            expect(PortOverviewWidget.prototype.render).to.have.been.calledOnce;
        });

        it('should render default widgets on model change', function() {
            this.portView.render();
            this.portView.model.trigger('change');
            this.portView.model.trigger('change');

            expect(PortInfoWidget.prototype.render).to.have.been.calledTwice;
            expect(PortOverviewWidget.prototype.render).to.have.been.calledOnce;
            expect(PortOverviewWidget.prototype.renderContent).to.have.been.calledTwice;
        });
    });

    describe('using mocks', function() {
        before(function() {
            this.dataSource = new DataSource('host', new Backbone.Model());
            this.stubApp = { dataSource: this.dataSource };
            this.stubApp.nav = new Backbone.Model({
                url_args: ['appId', 'operatorId', 'port name']
            });
        });

        beforeEach(function() {
            this.sandbox = sinon.sandbox.create();
            this.mockDataSource = this.sandbox.mock(this.dataSource);
        });

        afterEach(function() {
            this.sandbox.restore();
        });

        it('should get data from DataSource', function() {
            var mockPortData = {
                type: 'input',
                totalTuples: '10'
            };
            this.mockDataSource.expects('subscribe').once();

            var portView = new PortPageView({
                app: this.stubApp,
                pageParams: { appId: 'appId', operatorId: 'operatorId', portName: 'port name' }
            });

            //expect(portView.model.get('type')).to.equal(mockPortData.type);
            //expect(portView.model.get('totalTuples')).to.equal(mockPortData.totalTuples);

            this.mockDataSource.verify();
        });
    });
});

