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
var WidgetDefModel = require('./WidgetDefModel');

describe('WidgetDefModel.js', function() {

	var widget = new WidgetDefModel({
		width: 75
	});

	it('should prevent setting the width to something less than 20 or more than 100', function() {

		var less_than_20 = widget.set({'width':19},{validate:true});
		var more_than_100 = widget.set({'width':101},{validate:true});

		expect(less_than_20).to.equal(false);
		expect(more_than_100).to.equal(false);

	});

	it('should make height "auto" by default', function() {
	    expect(widget.get('height')).to.equal('auto');
	});

	it('should prevent setting height to anything besides a positive number or the string "auto"', function() {
	    var negative = widget.set({'height': -100}, {validate:true});
	    var string = widget.set({'height': 'not auto'}, {validate:true});
	    var number = widget.set({'height': 100}, {validate:true});
	    var auto = widget.set({'height': 'auto'}, {validate:true});

	    expect(negative).to.equal(false);
	    expect(string).to.equal(false);
	    expect(number).not.to.equal(false);
	    expect(auto).not.to.equal(false);
	});

});