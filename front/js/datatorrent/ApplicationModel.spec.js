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
var Backbone = require('backbone');
var DataSource = require('./DataSource');
var ApplicationModel = require('./ApplicationModel');

describe('ApplicationModel.js', function() {
    
    var sandbox, mockDataSource, appmodel;
    
    before(function() {
        mockDataSource = new DataSource('host', new Backbone.Model({}));
    });
    
    after(function() {
        
    });
    
    beforeEach(function() {
        sandbox = sinon.sandbox.create();
        appmodel = new ApplicationModel({},{ dataSource: mockDataSource});
        
    });
    
    afterEach(function() {
        sandbox.restore();
        appmodel = null;
    });
    
    it('should set dataSource as a property', function() {
        expect(appmodel.dataSource).to.equal(mockDataSource);
    });
    
    it('should set some defaults', function() {
        var tmp_model = new ApplicationModel({},{});
        var json = tmp_model.serialize();
        expect(json).to.have.property('state');
    });
    
    // describe('the fetch method', function() {
    //     
    // });

    describe('the serialize method', function() {
        
        it('should return an object', function() {
            expect(appmodel.serialize()).to.be.an('object');
        });
    });
    
});