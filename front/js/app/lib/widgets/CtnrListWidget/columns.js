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
/**
 * Column definitions for container list
*/

// host: "node5.morado.com"
// id: "container_1367286541553_0128_01_000035"
// jvmName: "11906@node5.morado.com"
// lastHeartbeat: "1368134861013"
// memoryMBAllocated: "16384"
// memoryMBFree: "1691"
// numOperators: "2"
// state: "ACTIVE"

var bormat = require('bormat');
var templates = DT.templates;
var formatters = DT.formatters;

var idFormatter = formatters.containerFormatter;

// Remove the @nodeX.morado.com suffix
var jvmName_rgx = /^(\d+)@(.*)/;
function processFormatter(value, row) {
    value = value || '';
    return value.replace(jvmName_rgx,'$1');
}

// Remove the pid from jvmName
function nodeFormatter(value, row) {
    value = value || '';
    return value.replace(jvmName_rgx, '$2');
}

function statusFormatter(value,row) {
    if (value == null || value === "")
        return "-";
    return '<span class="status-'+value.replace(' ','-').toLowerCase()+'">'+value+'</span>';
}

function nodeSorter(row1, row2) {
    var node1 = row1.get('jvmName').replace(jvmName_rgx, '$2');
    var node2 = row2.get('jvmName').replace(jvmName_rgx, '$2');
    if (node1 === node2) {
        return 0;
    }
    else if (node1 < node2) {
        return -1;
    }
    else {
        return 1;
    }
}

function heartbeatFormatter(value, row) {
    return new Date(value*1).toLocaleTimeString();
}

exports = module.exports = [
    { id: 'selector', label: '', key: 'selected', select: true, width: 40, lock_width: true },
    { id: 'id', label: 'Container Id', key: 'id', sort: 'string', filter: 'like', format: idFormatter, width: 70, sort_value: 'a' },
    { id: 'process_id', label: 'Process ID', key: 'jvmName', sort: 'string', filter: 'like', format: processFormatter },
    { id: 'node', label: 'Node Name', key: 'jvmName', sort: nodeSorter, filter: 'like', format: nodeFormatter },
    { id: 'lastHeartbeat', label: 'Last Heartbeat', key: 'lastHeartbeat', sort: 'number', filter: 'date', format: heartbeatFormatter },
    { id: 'memoryMBAllocated', label: 'Allocated Memory (MB)', key: 'memoryMBAllocated', format: 'commaInt', filter: 'number', sort: 'number' },
    { id: 'memoryMBFree', label: 'Free Memory (MB)', key: 'memoryMBFree', format: 'commaInt', filter: 'number', sort: 'number' },
    { id: 'numOperators', label: 'No. of Operators', key: 'numOperators', format: 'commaInt', filter: 'number', sort: 'number' },
    { id: 'state', label: 'State', key: 'state', sort: 'string', filter:'like', format: statusFormatter }
];