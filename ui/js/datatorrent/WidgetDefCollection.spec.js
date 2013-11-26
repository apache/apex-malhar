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
var WidgetDefCollection = require('./WidgetDefCollection');

describe('WidgetDefCollection.js', function() {

	var coll = new WidgetDefCollection([]);

	coll.order = [
		'id4',
		'id1',
		'id3',
		'id2'
	];

	coll.add([
		{ id: 'id1', widget: 'widget1', width: 100 },
		{ id: 'id2', widget: 'widget2', width: 100 },
		{ id: 'id3', widget: 'widget3', width: 100 },
		{ id: 'id4', widget: 'widget4', width: 100 }
	]);

	it('should respect this.order', function() {
		expect(coll.pluck('id')).to.eql(coll.order);
	});


});