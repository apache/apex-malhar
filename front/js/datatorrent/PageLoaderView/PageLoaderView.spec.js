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
var NavModel = require('./../NavModel/NavModel');
var PageLoaderView = require('./PageLoaderView');
var pages = require('../../app/pages');
var OpsHomePageView = require('../../app/lib/pages/OpsHomePageView');
var AppInstancePageView = require('../../app/lib/pages/AppInstancePageView');
var PhysOpPageView = require('../../app/lib/pages/PhysOpPageView');
var RecordingPageView = require('../../app/lib/pages/RecordingPageView');
var PortPageView = require('../../app/lib/pages/PortPageView');
var ErrorPageView = require('../../app/lib/pages/ErrorPageView');

describe('PageLoaderView.js', function() {

    describe('navigation with NavModel current_page', function() {
        before(function() {
            this.app = new Backbone.View();
            this.stubPageViewClass = Backbone.View.extend();
            this.pages = [
                {
                    'name': 'testpage',
                    'view': this.stubPageViewClass,
                    'routes': ['/route1','/route1/:param1'],
                    'paramList': ['param1'],
                    'mode': 'ops',
                    'breadcrumbs': [
                        {
                            name: 'home',
                            href: '#' 
                        },
                        { 
                            name: function(param1) { return 'param1: ' + param1; }, 
                            href: function(param1) { return '<a href="#' + param1 + '">' + param1 + '</a>'; }
                        },
                        {
                            name: 'here'
                        }
                    ]
                }
            ];
        });

        beforeEach(function() {
            this.sandbox = sinon.sandbox.create();
            this.navModel = new Backbone.Model({
                current_page: 'current page not set',
                url_args: []
            });
            this.navModel.modes = new Backbone.Collection([]);
        });

        afterEach(function() {
            this.sandbox.restore();
        });

        it('should instantiate page view on current_page change', function() {
            var spy = this.sandbox.spy(this.stubPageViewClass.prototype, 'initialize');
            var pageLoaderView = new PageLoaderView({
                pages: this.pages,
                app: this.app,
                model: this.navModel
            });
            this.navModel.set('current_page', 'testpage'); // trigger current_page change

            expect(this.stubPageViewClass.prototype.initialize).to.have.been.calledOnce;
        });

        it('should instantiate page view on url_args change', function() {
            var spy = this.sandbox.spy(this.stubPageViewClass.prototype, 'initialize');
            var pageLoaderView = new PageLoaderView({
                pages: this.pages,
                app: this.app,
                model: this.navModel
            });
            this.navModel.set('current_page', 'testpage', {silent: true}); // set current_page
            this.navModel.set('url_args', ['']); // trigger url_args change

            expect(this.stubPageViewClass.prototype.initialize).to.have.been.calledOnce;
        });
    });

    describe('navigation with Router', function() {
        var opts = { trigger: true, replace: true };

        before(function() {
            this.app = new Backbone.View();

        });

        beforeEach(function() {
            this.sandbox = sinon.sandbox.create();

            var navModel = new NavModel({}, {
                pages: pages
            });

            var pageLoaderView = new PageLoaderView({
                pages: pages,
                app: this.app,
                model: navModel
            });

            this.sandbox.stub(pageLoaderView, 'render');
            
            this.sandbox.stub(OpsHomePageView.prototype, 'initialize');
            this.sandbox.stub(OpsHomePageView.prototype, 'cleanUp');
            
            this.sandbox.stub(AppInstancePageView.prototype, 'initialize');
            this.sandbox.stub(AppInstancePageView.prototype, 'cleanUp');
            
            this.sandbox.stub(PhysOpPageView.prototype, 'initialize');
            this.sandbox.stub(PhysOpPageView.prototype, 'cleanUp');
            
            this.sandbox.stub(RecordingPageView.prototype, 'initialize');
            
            this.sandbox.stub(PortPageView.prototype, 'initialize');
            this.sandbox.stub(PortPageView.prototype, 'cleanUp');
            
            this.sandbox.stub(ErrorPageView.prototype, 'initialize');

            this.router = navModel.router;

            Backbone.history.start();
        });

        afterEach(function() {
            this.router.navigate('', opts);
            Backbone.history.stop();
            this.sandbox.restore();
        });

        it('should navigate to OpsHomePageView', function() {
            this.router.navigate('ops', opts);
            expect(OpsHomePageView.prototype.initialize).to.have.been.calledOnce;
        });

        it('should navigate to AppInstancePageView', function() {
            this.router.navigate('ops/apps/application_123', opts);
            expect(AppInstancePageView.prototype.initialize).to.have.been.calledOnce;
        });

        it('should navigate to PhysOpPageView', function() {
            this.router.navigate('ops/apps/application_123/operators/3', opts);
            expect(PhysOpPageView.prototype.initialize).to.have.been.calledOnce;
        });

        it('should navigate to RecordingPageView', function() {
            this.router.navigate('ops/apps/application_123/operators/3/recordings/567', opts);
            expect(RecordingPageView.prototype.initialize).to.have.been.calledOnce;
        });

        it('should navigate to PortPageView', function() {
            this.router.navigate('ops/apps/application_123/operators/3/ports/data', opts);
            expect(PortPageView.prototype.initialize).to.have.been.calledOnce;
        });

        it('should navigate to ErrorPageView', function() {
            this.router.navigate('ops/apps/application_123/invalidpath/3', opts);
            expect(ErrorPageView.prototype.initialize).to.have.been.calledOnce;
        });
    });
});