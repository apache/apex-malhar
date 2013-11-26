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
var RecordingModel = require('./RecordingModel');

describe('RecordingModel.js', function() {

	var recording, recording2, dataSource;

	describe('the initialize method', function() {

		beforeEach(function() {
			dataSource = {};
			recording = new RecordingModel({},{ dataSource: dataSource});
		});

		afterEach(function() {
			dataSource = recording = undefined;
		});

		it('should store the dataSource passed to it in the options', function() {
			expect(recording.dataSource).to.be.an('object');
		});

		it('should have port and tuple collections', function() {
			expect(recording.ports).to.be.an('object');
			expect(recording.tuples).to.be.an('object');
		});
		
		it('should ensure that attrs is an object', function() {
		    var fn = function() {
		        var rec = new RecordingModel();
		    }
		    expect(fn).not.to.throw();
		});

	});

});