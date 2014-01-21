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
var templates = require('./templates');
var bormat = require('bormat');

function containerFormatter(value, row) {
    if (!value) return '-';
    var vals = value.split('_');
    var displayval = vals[vals.length -1];
    return templates.container_link({
        containerId: value,
        appId: row.collection ? row.collection.appId : null,
        containerIdShort: displayval
    });
};

function windowFormatter(windowIdObj) {
    if (! ( /\d{5,}/.test(windowIdObj) ) ) return windowIdObj;
    if (typeof windowIdObj !== 'object') {
        windowIdObj = new WindowId(windowIdObj);
    }
    return windowIdObj.toString();
};

function windowOffsetFormatter(windowIdObj) {
    if (! ( /\d{5,}/.test(windowIdObj) ) ) return windowIdObj;
    if (typeof windowIdObj !== 'object') {
        windowIdObj = new WindowId(windowIdObj);
    }
    return windowIdObj.offset;
};

function statusClassFormatter(status) {
    return 'status-' + status.replace(/[^a-zA-Z]+/,'-').toLowerCase();
};

function logicalOpStatusFormatter(statuses) {
    var strings = _.map(statuses, function(val, key) {
        // return val.length + ' <span class="' + statusClassFormatter(key) + '">' + key + '</span>';
        return ' <span class="' + statusClassFormatter(key) + '" title="' + val.length + ' ' + key + '">' + val.length + '</span>';
    }, '');
    return strings.join(', ');
};

function percentageFormatter(value, isNumerator) {
    var multiplyBy = isNumerator ? 1 : 100;
    value = parseFloat(value).toFixed(3) * multiplyBy;
    value = value.toFixed(1);
    value = bormat.commaGroups(value);
    return value + '%';
};

function byteFormatter(bytes) {
    bytes *= 1;
    var precision = 1;
    var kilobyte = 1024;
    var megabyte = kilobyte * 1024;
    var gigabyte = megabyte * 1024;
    var terabyte = gigabyte * 1024;
   
    if ((bytes >= 0) && (bytes < kilobyte)) {
        return bytes + ' B';
 
    } else if ((bytes >= kilobyte) && (bytes < megabyte)) {
        return (bytes / kilobyte).toFixed(precision) + ' KB';
 
    } else if ((bytes >= megabyte) && (bytes < gigabyte)) {
        return (bytes / megabyte).toFixed(precision) + ' MB';
 
    } else if ((bytes >= gigabyte) && (bytes < terabyte)) {
        return (bytes / gigabyte).toFixed(precision) + ' GB';
 
    } else if (bytes >= terabyte) {
        return (bytes / terabyte).toFixed(precision) + ' TB';
 
    } else {
        return bytes + ' B';
    }
}

exports.containerFormatter = containerFormatter;
exports.windowFormatter = windowFormatter;
exports.windowOffsetFormatter = windowOffsetFormatter;
exports.statusClassFormatter = statusClassFormatter;
exports.logicalOpStatusFormatter = logicalOpStatusFormatter;
exports.percentageFormatter = percentageFormatter;
exports.commaGroups = bormat.commaGroups;
exports.timeSince = bormat.timeSince;
exports.byteFormatter = byteFormatter;