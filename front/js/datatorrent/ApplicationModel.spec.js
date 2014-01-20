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
var DataSource = require('./DataSource');
var BaseModel = require('./BaseModel');
var ApplicationModel = require('./ApplicationModel');
var OperatorCollection = require('./OperatorCollection');
var ContainerCollection = require('./ContainerCollection');
var WindowId = require('./WindowId');

describe('ApplicationModel.js', function() {
    
    var sandbox, mockDataSource, appmodel, fetchResponse;
    
    before(function() {
        mockDataSource = new DataSource('host', new Backbone.Model({}));
        sandbox = sinon.sandbox.create();
    });
    
    after(function() {
        
    });
    
    beforeEach(function() {

        appmodel = new ApplicationModel({},{ dataSource: mockDataSource});

        fetchResponse = function(options) {
            var updates = {
                'state': 'RUNNING',
                'startedTime': (+new Date()) + "",
                'elapsedTime': '72000'
            };
            appmodel.set(updates)
            options.success(appmodel, updates);
        }

        sandbox.stub(BaseModel.prototype, 'fetch', function() {
            return fetchResponse.apply(appmodel, arguments);
        });

        sandbox.stub(ContainerCollection.prototype, 'fetch', function() {

        });

        sandbox.stub(OperatorCollection.prototype, 'fetch', function() {

        });

    });
    
    afterEach(function() {
        sandbox.restore();
        appmodel = null;
    });
    
    it('should set dataSource as a property', function() {
        expect(appmodel.dataSource).to.equal(mockDataSource);
    });
    
    it('should set some defaults', function() {
        var json = appmodel.toJSON();
        _.each(['state', 'name', 'user'], function(attr) {
            expect(json).to.have.property(attr);
        });
    });

    _.each(['currentWindowId', 'recoveryWindowId'], function(windowIdKey) {

        it('should set the ' + windowIdKey + ' to an instance of WindowId by default', function() {
            expect(appmodel.get(windowIdKey)).to.be.instanceof(WindowId);
        });

        it('should not use the same ' + windowIdKey + ' object as other instantiated models', function() {
            var a2 = new ApplicationModel({},{ dataSource: mockDataSource});
            expect(appmodel.get(windowIdKey)).not.to.equal(a2.get(windowIdKey));
        });

    });

    describe('fetch method', function() {
        
        it('should call the BaseModel fetch method', function() {
            appmodel.fetch();
            expect(BaseModel.prototype.fetch).to.have.been.calledOnce;
        });

        it('should call fetch for containers and operators if they are present', function() {
            appmodel.setContainers([]);
            appmodel.setOperators([]);
            appmodel.fetch();
            expect(OperatorCollection.prototype.fetch).to.have.been.calledOnce;
            expect(ContainerCollection.prototype.fetch).to.have.been.calledOnce;
        });

        it('should NOT call fetch for containers and operators if they are NOT present', function() {
            appmodel.fetch();
            expect(OperatorCollection.prototype.fetch).not.to.have.been.called;
            expect(ContainerCollection.prototype.fetch).not.to.have.been.called;
        });        

        it('should call a success function supplied in the options', function() {

            var spy = sandbox.spy();
            
            appmodel.fetch({
                success: spy
            });
            
            expect(spy).to.have.been.calledOnce;
        });

    });

    describe('serialize method', function() {
        
        it('should return an object', function() {
            expect(appmodel.serialize()).to.be.an('object');
        });

        it('should add a lastHeartbeat attribute', function() {
            expect(appmodel.serialize().lastHeartbeat).to.be.a('number');
        });
        
    });


    
});