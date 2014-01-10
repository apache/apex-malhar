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
var WindowId = require('./WindowId');
var Model = require('./OperatorModel');

describe('OperatorModel.js', function() {
    
	var sandbox, m;

	beforeEach(function() {
	    sandbox = sinon.sandbox.create();
	    m = new Model({});
	});

	afterEach(function() {
	    sandbox.restore();
	    m = null;
	});

	_.each(['currentWindowId', 'recoveryWindowId'], function(windowIdKey) {

        it('should set the ' + windowIdKey + ' to an instance of WindowId by default', function() {
            expect(m.get(windowIdKey)).to.be.instanceof(WindowId);
        });

        it('should not use the same ' + windowIdKey + ' object as other instantiated models', function() {
            var m2 = new Model({});
            expect(m.get(windowIdKey)).not.to.equal(m2.get(windowIdKey));
        });

    });

    describe('the isRecording method', function() {
        it('should return true if recordingStartTime is not -1', function() {
            m.set('recordingStartTime', +new Date() + '');
            expect(m.isRecording()).to.eql(true);
        });

        it('should return true if one of its ports has a recordingStartTime', function() {
            m.set('ports', [
            	{ recordingStartTime: (+new Date() - 1000)+ ''},
            ]);
           	expect(m.isRecording()).to.eql(true); 
        });

        it('should return false if recordingStartTime is not present or is -1', function() {
            m.set('recordingStartTime', '-1');
            expect(m.isRecording()).to.eql(false);
            m.unset('recordingStartTime');
            expect(m.isRecording()).to.eql(false);
        });
    });

});