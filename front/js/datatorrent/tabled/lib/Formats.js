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
var Bormats = require('bormat');
exports.select = function(value, model, column) {
    var select_key = column.get('select_key');
    return '<input type="checkbox" class="selectbox" data-id="' + model.id + '" ' + (model[select_key] ? 'checked="checked"' : '') + '>';
}

var timeSince = Bormats.timeSince;

exports.timeSince = function(value) {
    if (/^\d+$/.test(value)) {
        var newVal = timeSince(value) || "a moment";
        return '<span title="' + new Date(value * 1).toLocaleString() + '">' + newVal + ' ago</span>';
    }
    return value;
}

exports.unixUptime = function(value) {
	if (/^\d+$/.test(value)) {
        var newVal = timeSince({
        	timeStamp: value,
        	unixUptime: true
        }) || "a moment ago";
        return newVal;
    }
    return value;	
}

exports.timeStamp = function(value) {

	var now = +new Date();
	value *= 1;

	// if more than a day ago, do full date
	if ( now - value > 1000*60*60*24 ) {
		return new Date(value).toLocaleString();
	} else {
		return new Date(value).toLocaleTimeString();
	}
}

exports.commaInt = Bormats.commaGroups;