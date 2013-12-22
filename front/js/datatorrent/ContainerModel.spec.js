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
var ContainerModel = require('./ContainerModel');
var DataSource = require('./DataSource/DataSource');

describe('ContainerModel.js', function() {

    var container, 
        sandbox, 
        mockDataSource, 
        containerId = 'container_00000000000_0001_000001', 
        appId = 'application_00000000000_0001',
        response = {
            host: 'node1.morado.com',
            id: containerId,
            jvmName: '0001@node1.morado.com',
            lastHeartbeat: '1374172420819',
            memoryMBAllocated: 100,
            memoryMBFree: 50,
            numOperators: 2,
            state: 'ACTIVE'
        };
    
    before(function() {
        mockDataSource = new DataSource('host', new Backbone.Model({}));
    });
    
    beforeEach(function() {
        sandbox = sinon.sandbox.create();
        container = new ContainerModel({
            appId: appId,
            id: containerId
        }, {
            dataSource: mockDataSource
        });
    });
    
    afterEach(function() {
        container = null;
        sandbox.restore();
    });
    
    describe('the constructor', function() {
        
        it('should set the dataSource', function() {
            expect(container.dataSource).to.equal(mockDataSource);
        });
        
    });

    describe('updateOpAggValues method', function() {
        
        it('should call set', function() {
            var spy = sandbox.spy();
            var ctx = {
                operators: {
                    toJSON: function() { return [] }
                },
                set: spy
            };

            ContainerModel.prototype.updateOpAggValues.call(ctx);
            expect(spy).to.have.been.calledOnce;
        });

        it('should set windowId attributes to 0 if no operators are present', function() {
            var spy = sandbox.spy();
            var ctx = {
                operators: {
                    toJSON: function() { return [] }
                },
                set: spy
            };

            ContainerModel.prototype.updateOpAggValues.call(ctx);

            var aggregates = spy.getCall(0).args[0];
            expect(aggregates).to.have.property('currentWindowId', '0');
            expect(aggregates).to.have.property('recoveryWindowId', '0');
        });

    });
    
});