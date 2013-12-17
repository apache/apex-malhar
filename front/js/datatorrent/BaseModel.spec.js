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
var settings = require('./settings');
var BaseModel = require('./BaseModel');
var BaseUtil = require('./BaseUtil');
var WindowId = require('./WindowId');

describe('BaseModel.js', function() {
    
	var sandbox, m;

	beforeEach(function() {
	    sandbox = sinon.sandbox.create();
	    m = new BaseModel({});
	});

	afterEach(function() {
	    sandbox.restore();
	});

   	_.each(['resourceURL', 'resourceTopic', 'resourceAction'], function(method) {
       it('should have a ' + method + ' method', function() {
           expect(BaseModel.prototype[method]).to.be.a('function');
           expect(BaseModel.prototype[method]).to.equal(BaseUtil[method]);
       });
	});

	it('should have the settings object attached to it', function() {
		expect(BaseModel.prototype.settings).to.equal(settings);
	});

	it('should set all keys enumerated by the windowIdProperties to windowId instances', function() {
		_.each(BaseModel.prototype.windowIdProperties, function(windowKey) {
			var updates = {};
			updates[windowKey] = '5958104595812065129';
			m.set(updates);
			expect(m.get(windowKey)).to.be.instanceof(WindowId);
		});
	});

	it('should not create a new windowId instance on a key if it already exists', function() {
	    _.each(BaseModel.prototype.windowIdProperties, function(windowKey) {
	    	m.set(windowKey, '5958104595812065129');
	    	var w = m.get(windowKey);
	    	m.set(windowKey, '0000000000000000000');
	    	expect(m.get(windowKey)).to.equal(w);
	    });
	});

	it('should still trigger a change:propertyname event when a windowId has been updated', function() {
		var spy = sandbox.spy();
	   	_.each(BaseModel.prototype.windowIdProperties, function(windowKey) {
	    	m.set(windowKey, '5958104595812065129');
	    	m.on('change:' + windowKey, spy);
	    	m.set(windowKey, '0000000000000000000');
	    });
	    expect(spy.callCount).to.equal(BaseModel.prototype.windowIdProperties.length);
	});

});