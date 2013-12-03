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
    var recordingStartTime = row.get('recordingStartTime');
    return (recordingStartTime && recordingStartTime != '-1') ? '<i class="icon-rec-on"></i> ' + value : value ;
}

function idFormatter(value, row) {
    value = templates.phys_op_link({ appId: row.collection.appId, operatorId: value, displayText: value });
    return value;
}

function cpuFormatter(value) {
    return formatters.percentageFormatter(value, true);
}

exports = module.exports = [
    { id: "selector", key: "selected", label: "", select: true, width: 40, lock_width: true },
    { id: "id", label: "Operator ID", key: "id", sort: "number", filter: "number", width:55, sort_value: "a", format: idFormatter },
    { id: "name", label: "Name", key: "name", sort: "string", filter: "like", width:150, format: nameFormatter },
    { id: "tuplesProcessedPSMA", label: "Processed/sec", key: "tuplesProcessedPSMA", format: "commaInt", sort: "number", filter: "number" },
    { id: "totalTuplesProcessed", label: "Processed total", key: "totalTuplesProcessed", format: "commaInt", sort: "number", filter: "number" },
    { id: "tuplesEmittedPSMA", label: "Emitted/s", key: "tuplesEmittedPSMA", format: "commaInt", sort: "number", filter: "number" },
    { id: "totalTuplesEmitted", label: "Emitted total", key: "totalTuplesEmitted", format: "commaInt", sort: "number", filter: "number" },
    { id: "status", label: "Status", key: "status", format: statusFormatter, sort: "string", filter:"like" },
    { id: "container", label: "Container", key: "container", format: containerFormatter, sort: "string", filter:"like" },
    { id: "host", label: "Node", key: "host", sort: "string", filter:"like" },
    { id: "latency", label: "Latency", key: "latencyMA", sort: "number", filter: "number" },
    // { id: "failureCount", label: "Failure Count", key: "failureCount", sort: "number", filter: "number" },
    { id: "currentWindowId", label: "Current Window ID", key: "currentWindowId", sort: "number", filter: "number", format: windowFormatter },
    { id: "recoveryWindowId", label: "Recovery Window ID", key: "recoveryWindowId", sort: "number", filter: "number", format: windowFormatter },
    { id: "cpuPercentageMA", label: "CPU %", key: "cpuPercentageMA", sort: "number", filter: "number", format: cpuFormatter }
]