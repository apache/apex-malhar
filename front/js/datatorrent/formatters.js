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
}

function windowFormatter(windowIdObj) {
    if (! ( /\d{5,}/.test(windowIdObj) ) ) return windowIdObj;
    if (typeof windowIdObj !== 'object') {
        windowIdObj = new WindowId(windowIdObj);
    }
    return windowIdObj.toString();
}

function windowOffsetFormatter(windowIdObj) {
    if (!_.isObject(windowIdObj)) {
      windowIdObj = new WindowId(windowIdObj);
    }

    return windowIdObj.offset;
}

function statusClassFormatter(status) {
    return 'status-' + status.replace(/[^a-zA-Z]+/,'-').toLowerCase();
}

function logicalOpStatusFormatter(statuses) {
    var strings = _.map(statuses, function(val, key) {
        // return val.length + ' <span class="' + statusClassFormatter(key) + '">' + key + '</span>';
        return ' <span class="' + statusClassFormatter(key) + '" title="' + val.length + ' ' + key + '">' + val.length + '</span>';
    }, '');
    return strings.join(', ');
}

function percentageFormatter(value, isNumerator) {
    var multiplyBy = isNumerator ? 1 : 100;
    value = parseFloat(value).toFixed(3) * multiplyBy;
    value = value.toFixed(1);
    value = bormat.commaGroups(value);
    return value + '%';
}

// holds number of bytes per level
var levels = { b: 1 };
levels.kb = 1024;
levels.mb = levels.kb * 1024;
levels.gb = levels.mb * 1024;
levels.tb = levels.gb * 1024;
/**
 * Format a given number of bytes (or other unit)
 * @param  {number} bytes The number of bytes (or other unit) to format
 * @param  {string} level (optional) units of the bytes parameter (defaults to "b" for bytes, can be "kb", "mb", "gb", or "tb")
 * @return {string} returns human-readable string format
 */
function byteFormatter(bytes, level) {
    var precision = 1;
    level = level || 'b';
    if (!levels.hasOwnProperty(level)) {
        throw new TypeError('byteFormatter 2nd argument must be one of the following: "b","kb","mb","gb","tb"');
    }
    bytes *= levels[level];
   
    if ((bytes >= 0) && (bytes < levels.kb)) {
        return bytes + ' B';
 
    } else if ((bytes >= levels.kb) && (bytes < levels.mb)) {
        return (bytes / levels.kb).toFixed(precision) + ' KB';
 
    } else if ((bytes >= levels.mb) && (bytes < levels.gb)) {
        return (bytes / levels.mb).toFixed(precision) + ' MB';
 
    } else if ((bytes >= levels.gb) && (bytes < levels.tb)) {
        return (bytes / levels.gb).toFixed(precision) + ' GB';
 
    } else if (bytes >= levels.tb) {
        return (bytes / levels.tb).toFixed(precision) + ' TB';
 
    } else {
        return bytes + ' B';
    }
}

function cpusFormatter(percent, isNumerator) {
    if (isNumerator) {
        percent /= 100;
    } else {
        percent *= 1;
    }
    percent = percent.toFixed(2);
    return percent + '';
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
exports.cpusFormatter = cpusFormatter;