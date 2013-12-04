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
var formatters = DT.formatters;
var windowFormatter = formatters.windowFormatter;

// function containersFormatter(ctnrs, row) {
// 	var links = _.map(ctnrs, function(ctnr) {
// 		return formatters.containerFormatter(ctnr, row);
// 	});
// 	return links.join(",");
// }
function cpuFormatter(value) {
    return formatters.percentageFormatter(value, true);
}

function heartbeatFormatter(value) {
    return new Date(value*1).toLocaleTimeString();
}

function statusFormatter(value,row) {
    var strings = _.map(value, function(val, key) {
        return val.length + ' <span class="' + formatters.statusClassFormatter(key) + '">' + key + '</span>';
    }, '');
    return strings.join(', ');
}

exports = module.exports = [
	{ id: 'selector', key: 'selected', label: '', select: true, width: 40, lock_width: true },
	{ id: 'logicalName', key: 'logicalName', label: 'name', sort: 'string', filter: 'like' },
    { id: 'className', key: 'className', label: 'class', sort: 'string', filter: 'like' },
    { id: 'cpuPercentageMA', key: 'cpuPercentageMA', label: 'CPU %', sort: 'number', filter: 'number', format: cpuFormatter },
    { id: 'currentWindowId', key: 'currentWindowId', label: 'current window', sort: 'string', filter: 'like', format: windowFormatter },
    { id: 'recoveryWindowId', key: 'recoveryWindowId', label: 'recovery window', sort: 'string', filter: 'like', format: windowFormatter },
    { id: 'failureCount', key: 'failureCount', label: 'failure count', sort: 'string', filter: 'like' },
    { id: 'lastHeartbeat', key: 'lastHeartbeat', label: 'last heartbeat', sort: 'date', filter: 'date', format: heartbeatFormatter },
    { id: 'latencyMA', key: 'latencyMA', label: 'latency (ms)', sort: 'number', filter: 'number' },
    { id: 'status', key: 'status', label: 'status', sort: 'string', filter: 'like', format: statusFormatter },
    { id: 'tuplesProcessedPSMA', key: 'tuplesProcessedPSMA', label: DT.text('processed_per_sec'), sort: 'number', filter: 'number', format: 'commaInt' },
    { id: 'tuplesEmittedPSMA', key: 'tuplesEmittedPSMA', label: DT.text('emitted_per_sec'), sort: 'number', filter: 'number', format: 'commaInt' },
    { id: 'totalTuplesProcessed', key: 'totalTuplesProcessed', label: DT.text('processed_total'), sort: 'number', filter: 'number', format: 'commaInt' },
    { id: 'totalTuplesEmitted', key: 'totalTuplesEmitted', label: DT.text('emitted_total'), sort: 'number', filter: 'number', format: 'commaInt' }
    // { id: 'hosts', key: 'hosts', label: 'hosts', sort: 'string', filter: 'like' },
    // { id: 'ids', key: 'ids', label: 'ids', sort: 'string', filter: 'like' },
    // { id: 'ports', key: 'ports', label: 'ports', sort: 'string', filter: 'like' },
    // { id: 'containers', key: 'containers', label: 'containers', sort: 'string', filter: 'like', format: containersFormatter },
];
