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
var templates = DT.templates;
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

function logicalNameFormatter(value, row) {

    var appId = row.get('appId') || ( row.collection ? row.collection.appId : false );

    if (appId) {
        return templates.logical_op_link({
            appId: appId,
            logicalName: value
        });    
    }
    return value;
}

exports = module.exports = [
	{ id: 'selector', key: 'selected', label: '', select: true, width: 40, lock_width: true },
	{ id: 'logicalName', key: 'logicalName', label: 'name', sort: 'string', sort_value: 'a', filter: 'like', format: logicalNameFormatter, width: 100 },
    { id: 'className', key: 'className', label: 'class', sort: 'string', filter: 'like', width: 250 },
    { id: 'cpuMin', key: 'cpuMin', label: DT.text('cpu_min_label'), sort: 'number', filter: 'number', format: cpuFormatter },
    { id: 'cpuMax', key: 'cpuMax', label: DT.text('cpu_max_label'), sort: 'number', filter: 'number', format: cpuFormatter },
    { id: 'cpuAvg', key: 'cpuAvg', label: DT.text('cpu_avg_label'), sort: 'number', filter: 'number', format: cpuFormatter },
    { id: 'currentWindowId', key: 'currentWindowId', label: DT.text('current_wid_label'), sort: 'string', filter: 'like', format: windowFormatter },
    { id: 'recoveryWindowId', key: 'recoveryWindowId', label: DT.text('recovery_wid_label'), sort: 'string', filter: 'like', format: windowFormatter },
    { id: 'failureCount', key: 'failureCount', label: DT.text('failure_count_label'), sort: 'string', filter: 'like' },
    { id: 'lastHeartbeat', key: 'lastHeartbeat', label: 'last heartbeat', sort: 'date', filter: 'date', format: heartbeatFormatter },
    { id: 'latencyMA', key: 'latencyMA', label: DT.text('latency_ms_label'), sort: 'number', filter: 'number' },
    { id: 'status', key: 'status', label: DT.text('status_label'), sort: 'string', filter: 'like', format: formatters.logicalOpStatusFormatter },
    { id: 'tuplesProcessedPSMA', key: 'tuplesProcessedPSMA', label: DT.text('processed_per_sec'), sort: 'number', filter: 'number', format: 'commaInt' },
    { id: 'tuplesEmittedPSMA', key: 'tuplesEmittedPSMA', label: DT.text('emitted_per_sec'), sort: 'number', filter: 'number', format: 'commaInt' },
    { id: 'totalTuplesProcessed', key: 'totalTuplesProcessed', label: DT.text('processed_total'), sort: 'number', filter: 'number', format: 'commaInt', width: 100 },
    { id: 'totalTuplesEmitted', key: 'totalTuplesEmitted', label: DT.text('emitted_total'), sort: 'number', filter: 'number', format: 'commaInt', width: 100 }
    // { id: 'hosts', key: 'hosts', label: 'hosts', sort: 'string', filter: 'like' },
    // { id: 'ids', key: 'ids', label: 'ids', sort: 'string', filter: 'like' },
    // { id: 'ports', key: 'ports', label: 'ports', sort: 'string', filter: 'like' },
    // { id: 'containers', key: 'containers', label: 'containers', sort: 'string', filter: 'like', format: containersFormatter },
];
