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
var _ = require('underscore'), Backbone = require('backbone');
var BasePageView = DT.lib.BasePageView;

describe('BasePageView.js', function() {
    
    var ChildPage, 
        ChildPageWithMgr, 
        pageInstance, 
        pageInstance2, 
        mockApp, 
        mockDataSource, 
        defaultDashes,
        sandbox;
    
    beforeEach(function() {
        
        sandbox = sinon.sandbox.create();
        
        defaultDashes = [
            {
                dash_id: "default",
                widgets: [
                    { widget: "FirstWidget", id: "first widget" },
                    { widget: "SecondWidget", id: "second widget" }
                ]
            }
        ];
        
        mockDataSource = {};
        
        mockApp = {
            dataSource: mockDataSource
        };
        
        ChildPage = BasePageView.extend({
            pageName: 'ChildPage',
            defaultDashes: defaultDashes
        });
        
        ChildPageWithMgr = BasePageView.extend({
            pageName: 'ChildPage2',
            useDashMgr: true,
            defaultDashes: defaultDashes
        });
        
    });
    
    afterEach(function() {
        
        _.each([pageInstance, pageInstance2], function(page) {
            localStorage.removeItem(page.__lsPrefix + ".dashboards");
        });
        
        ChildPage = null;
        
        pageInstance = null;
        
        pageInstance2 = null;
        
        sandbox.restore();
        
    });
    
    describe('the constructor', function() {
        
        beforeEach(function(){
            pageInstance = new ChildPage({
                app: mockApp,
                dataSource: mockDataSource
            });
        
            pageInstance2 = new ChildPageWithMgr({
                app: mockApp,
                dataSource: mockDataSource
            });
        });
        
        it('should throw if no pageName is specified', function() {
            var BadChild = BasePageView.extend({
                
            });
            
            var fn = function() {
                new BadChild({
                    app: mockApp,
                    dataSource: mockDataSource
                });
            }
            expect(fn).to.throw();
        });
        
        _.each(['app', 'dataSource', '__lsPrefix','__wDashes', '__wClasses'], function(key) {
            it('should set ' + key + ' on itself', function() {
                expect(pageInstance).to.have.property(key);
            });
        });
            
        _.each(['__wClasses', '__wDashes'], function(key) {
            it(key + ' should be backbone collection', function() {
               expect(pageInstance[key]).to.be.instanceof(Backbone.Collection) 
            });    
        });

        
        
        it('should set a subview for dashMgr if it has been specified', function() {
            expect(pageInstance.subview('dashMgr')).to.equal(undefined);
            expect(pageInstance2.subview('dashMgr')).to.be.instanceof(Backbone.View);
        });
        
    });
    
    describe('the loadDashboards method', function() {
        
        beforeEach(function(){
            
            sandbox.stub(ChildPage.prototype, "loadDash");
            
            pageInstance = new ChildPage({
                app: mockApp,
                dataSource: mockDataSource
            });
        
            pageInstance2 = new ChildPageWithMgr({
                app: mockApp,
                dataSource: mockDataSource
            });
        });

        it('should load up the default dashboards', function() {
            pageInstance.loadDashboards('default');
            expect(pageInstance.__wDashes.length).to.equal(1);
        });
        
        it('should call loadDash', function() {
            pageInstance.loadDashboards('default');
            expect(ChildPage.prototype.loadDash).to.have.been.calledOnce;
        });
        
    });
    
    describe('the saveDashes function', function() {
        
        beforeEach(function() {
            sandbox.stub(ChildPage.prototype, "saveDashes");
            
            pageInstance = new ChildPage({
                app: mockApp,
                dataSource: mockDataSource
            });
        
            pageInstance2 = new ChildPageWithMgr({
                app: mockApp,
                dataSource: mockDataSource
            });
            
            pageInstance.loadDashboards('default');
        });
        
        it('should be called when a widget definition has been changed', function() {
            var curdash = pageInstance.__wDashes.find(function(dash){ return dash.get('selected') });
            var widgets = curdash.get('widgets');
            widgets.at(0).set('width', 50);
            expect(ChildPage.prototype.saveDashes).to.have.been.calledTwice;
        });
        
    });
    
});