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
var NavModel = require('./NavModel');
var util = require('./util');

describe('NavModel.js', function() {
    
    var sandbox, navModel, opts, PageOne, pages, modes;
    
    beforeEach(function() {
        
        sandbox = sinon.sandbox.create();
        
        opts = { trigger: true, replace: true };
    
        PageOne = Backbone.View.extend({ pageName: 'pageOne' });
    
        pages = [
            {
                'name': 'pageOne',
                'view': PageOne,
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
            },
            {
                'name': 'pageTwo',
                'view': PageOne,
                'routes': ['/route2','/route2/:param1'],
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

        modes = [{
            'name': 'Configuration',
            'href': '#config',
            'id': 'config'
        }, {
            'name': 'Development',
            'href': '#dev',
            'id': 'dev'
        }, {
            'name': 'Operations',
            'href': '#ops',
            'id': 'ops'
        }];
        
        navModel = new NavModel({},{ pages: pages });
    });

    afterEach(function() {
        
        sandbox.restore();
        navModel = opts = pages = null;
        Backbone.history.stop();
        window.location.hash = '';
        
    });
    
    describe('public methods', function() {
        describe('the constructor', function() {
            it('should call util.extractRoutesFrom with pages', function() {
        
                sandbox.spy(util, 'extractRoutesFrom');
                new NavModel({}, { pages: pages });
                expect(util.extractRoutesFrom).to.have.been.calledOnce;
                expect(util.extractRoutesFrom.getCall(0).args[0]).to.equal(pages);
        
            });
    
            it('should create a router object', function() {
                expect(navModel.router).to.be.instanceof(Backbone.Router);
            });

            it('should create a modes collection', function() {
                var m = new NavModel({}, { pages: pages, modes: modes });
                expect(m.modes).to.be.instanceof(Backbone.Collection);
            });
        });
    
        describe('the onRouteChange method', function() {
        
            it('should be triggered when the "route" event occurs on the router', function() {
                sandbox.stub(NavModel.prototype, 'onRouteChange');
                var nav = new NavModel({}, { pages: pages });
                nav.router.trigger('route', 'pageOne', []);
                expect(nav.onRouteChange).to.have.been.calledOnce;
            });
    
            it('should change current_page on page re-route and url_args when not changing pages', function() {

                sandbox.spy(NavModel.prototype, 'onRouteChange');

                var nav = new NavModel({}, { pages: pages });
                var listener = _.extend({}, Backbone.Events), pageSpy, urlSpy;
        
                listener.listenTo(nav, 'change:current_page', pageSpy = sandbox.spy() );
                listener.listenTo(nav, 'change:url_args', urlSpy = sandbox.spy() );
                nav.router.trigger('route', 'pageOne', []);
                expect(nav.get('current_page')).to.equal('pageOne');
                expect(pageSpy).to.have.been.calledOnce;
            
                nav.router.trigger('route','pageOne', ['testing']);
                expect(nav.get('url_args')).to.eql(['testing']);
                expect(urlSpy).to.have.been.calledOnce;
            });
        
        });
    
        describe('the start method', function() {
            it('should call Backbone.history.start', function() {
                sandbox.stub(Backbone.history, 'start');
                navModel.start();
                expect(Backbone.history.start).to.have.been.calledOnce;
            });
        });
    
        describe('the go method', function() {
            it('should trigger navigate on the router with the same params', function() {
                sandbox.stub(navModel.router, 'navigate');
                navModel.go('here', true);
                expect(navModel.router.navigate).to.have.been.calledOnce;
                expect(navModel.router.navigate.getCall(0).args[0]).to.equal('here');
                expect(navModel.router.navigate.getCall(0).args[1]).to.equal(true);
            });
        });
    });
    
    describe('private methods', function() {
        
        describe('the extractRoutesFrom method', function() {
            
            it('should create a routes object from the pages array', function() {
                
                var res = util.extractRoutesFrom(pages);
                expect(res).to.eql({
                    '/route1': 'pageOne',
                    '/route1/:param1': 'pageOne',
                    '/route2': 'pageTwo',
                    '/route2/:param1': 'pageTwo'
                });
                
            });
            
        });
        
    });
    
});