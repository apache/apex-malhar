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
 * Column definitions for port list
*/

// bufferServerBytesPSMA10: "",
// name: "",
// totalTuples: "",
// tuplesPSMA10: "",
// type: ""
var templates = DT.templates;
var colors = {
    'input': '#178DB8',
    'output': '#52B328'
}

function typeFormatter(value, row) {
    var color = colors.hasOwnProperty(value) ? colors[value] : '#000';
    return '<span style="font-weight: bold; color: '+color+';">'+value+'</span>';
}

function nameFormatter(name, row) {
    var appId = row.collection.appId;
    var operatorId = row.collection.operatorId;
    return templates.port_name_link({
        portName: name,
        appId: appId,
        operatorId: operatorId
    });
}

exports = module.exports = [
    { id: 'selector', label: '', key: 'selected', select: true, width: 40, lock_width: true },
    { id: 'name', key: 'name', label: DT.text('name_label'), filter: 'like', sort: 'string', format: nameFormatter, sort_value: 'a' },
    { id: 'type', key: 'type', label: DT.text('type_label'), filter: 'like', sort: 'string', format: typeFormatter },
    { id: 'tuplesPSMA', key: 'tuplesPSMA', label: DT.text('tuples_per_sec'), filter: 'number', sort: 'number', format: 'commaInt' },
    { id: 'totalTuples', key: 'totalTuples', label: DT.text('tuples_total'), filter: 'number', sort: 'number', format: 'commaInt' },
    { id: 'bufferServerBytesPSMA', key: 'bufferServerBytesPSMA', label: DT.text('buffer_server_bps_label'), filter: 'number', sort: 'number', format: 'commaInt' }
];