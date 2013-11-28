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
var WindowId = require('./WindowId');
var templates = require('./templates');
var bormat = require('bormat');

exports.containerFormatter = function(value, row) {
    if (!value) return '-';
    var vals = value.split('_');
    var displayval = vals[vals.length -1];
    return templates.container_link({
        containerId: value,
        appId: row.collection ? row.collection.appId : null,
        containerIdShort: displayval
    });
};

exports.windowFormatter = function(windowIdObj) {
    if (! ( /\d{5,}/.test(windowIdObj) ) ) return windowIdObj;
    if (typeof windowIdObj !== 'object') {
        windowIdObj = new WindowId(windowIdObj);
    }
    return '<span title="'+windowIdObj.value+'">'+windowIdObj.offset+'</span>';
};

exports.windowOffsetFormatter = function(windowIdObj) {
    if (! ( /\d{5,}/.test(windowIdObj) ) ) return windowIdObj;
    if (typeof windowIdObj !== 'object') {
        windowIdObj = new WindowId(windowIdObj);
    }
    return windowIdObj.offset;
};

exports.statusClassFormatter = function(status) {
    return 'status-' + status.replace(/[^a-zA-Z]+/,'-').toLowerCase();
};

exports.percentageFormatter = function(value, isNumerator) {
    var multiplyBy = isNumerator ? 1 : 100;
    value = parseFloat(value).toFixed(3) * multiplyBy;
    value = value.toFixed(2);
    value = bormat.commaGroups(value);
    return value + '%';
};

exports.commaGroups = bormat.commaGroups;
exports.timeSince = bormat.timeSince;