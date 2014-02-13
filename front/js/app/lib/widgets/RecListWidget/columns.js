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
var kt = require('knights-templar');
var bormat = require('bormat');
var templates = DT.templates;
var portInteractions;

function endedFormatter(value, row) {
    return value ? 'yes' : 'no' ;
}

function EndedSorter(row1, row2) {
    var e1 = row1.get('ended');
    var e2 = row2.get('ended');
    if (e1 && !e2) return -1;
    if (!e1 && e2) return 1;
    return 0;
}

function nameFormatter(name, row) {
    var appId = row.collection.appId;
    var operatorId = row.collection.operatorId;
    var startTime = row.get('startTime') * 1;
    var toStringFn = +new Date() - startTime < 86400000 ? 'toLocaleTimeString' : 'toLocaleString';
    return templates.recording_name_link({
        recordingName: name || new Date(startTime)[toStringFn](),
        ended: row.get('ended'),
        appId: row.get('appId'),
        operatorId: row.get('operatorId'),
        startTime: startTime
    });
}

function portsFormatter(ports, row) {
    var port_string = row.ports.map(function(port) { return port.get('name') }).join('\n');
    return '<span class="port-list-tooltip" title="' + port_string + '">' + row.ports.length + ' <small>(hover for list)</small></span>';
}

exports = module.exports = [
    { id: "selector", key: "selected", label: "", select: true, width: 40, lock_width: true },
    { id: "recordingName", key: "recordingName", label: DT.text('name_label'), filter: "like", sort: "string", format: nameFormatter },
    { id: "ports", key: "ports", label: DT.text('ports_label'), filter: "likeFormatted", format: portsFormatter, interaction: portInteractions },
    { id: "startTime", key: "startTime", label: DT.text('started_label'), format: bormat.timeSince, sort: "number", filter: "date" },
    { id: "ended", key: "ended", label: DT.text('ended_label'), format: endedFormatter, filter: "likeFormatted", sort: EndedSorter },
    { id: "operatorId", key: "operatorId", label: DT.text('operator_id_label'), filter: "number", sort: "number" },
    { id: "totalTuples", key: "totalTuples", label: DT.text('total_tuples_label'), format: bormat.commaGroups, sort: "number", filter: "number" }
]