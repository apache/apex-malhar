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
var formatters = DT.formatters;
var templates = DT.templates;

var containerFormatter = formatters.containerFormatter;

var windowFormatter = formatters.windowFormatter;

function statusFormatter(value,row) {
    if (value == null || value === "")
        return "-";
    return '<span class="' + formatters.statusClassFormatter(value) + '">' + value + '</span>';
}

function nameFormatter(value, row) {
    value = templates.logical_op_link({
        appId: row.collection.appId,
        logicalName: value
    });
    return row.isRecording() ? '<i class="icon-rec-on"></i> ' + value : value ;
}

function idFormatter(value, row) {
    value = templates.phys_op_link({ appId: row.collection.appId, operatorId: value, displayText: value });
    return value;
}

function cpuFormatter(value) {
    return formatters.percentageFormatter(value, true);
}

exports = module.exports = [
    { id: 'selector', key: 'selected', label: '', select: true, width: 40, lock_width: true },
    { id: 'id', label: DT.text('id_label'), key: 'id', sort: 'number', filter: 'number', width:55, sort_value: 'a', format: idFormatter },
    { id: 'name', label: DT.text('name_label'), key: 'name', sort: 'string', filter: 'like', width:150, format: nameFormatter },
    { id: 'tuplesProcessedPSMA', label: DT.text('processed_per_sec'), key: 'tuplesProcessedPSMA', format: 'commaInt', sort: 'number', filter: 'number' },
    { id: 'totalTuplesProcessed', label: DT.text('processed_total'), key: 'totalTuplesProcessed', format: 'commaInt', sort: 'number', filter: 'number', width: 100 },
    { id: 'tuplesEmittedPSMA', label: DT.text('emitted_per_sec'), key: 'tuplesEmittedPSMA', format: 'commaInt', sort: 'number', filter: 'number'},
    { id: 'totalTuplesEmitted', label: DT.text('emitted_total'), key: 'totalTuplesEmitted', format: 'commaInt', sort: 'number', filter: 'number', width: 100  },
    { id: 'status', label: DT.text('status_label'), key: 'status', format: statusFormatter, sort: 'string', filter:'like' },
    { id: 'container', label: DT.text('container_label'), key: 'container', format: containerFormatter, sort: 'string', filter:'like' },
    { id: 'host', label: DT.text('host_label'), key: 'host', sort: 'string', filter:'like', width: 130 },
    { id: 'latency', label: DT.text('latency_ms_label'), key: 'latencyMA', sort: 'number', filter: 'number' },
    // { id: 'failureCount', label: DT.text('failure_count_label'), key: 'failureCount', sort: 'number', filter: 'number' },
    { id: 'currentWindowId', label: DT.text('current_wid_label'), key: 'currentWindowId', sort: 'number', filter: 'number', format: windowFormatter },
    { id: 'recoveryWindowId', label: DT.text('recovery_wid_label'), key: 'recoveryWindowId', sort: 'number', filter: 'number', format: windowFormatter },
    { id: 'cpuPercentageMA', label: DT.text('cpu_percentage_label'), key: 'cpuPercentageMA', sort: 'number', filter: 'number', format: cpuFormatter }
]