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

function portFormat(name, port) {
    var operator = port.get('operator');
    return templates.port_name_link({ appId: operator.get('appId'), operatorId: operator.get('id'), portName: name });
}

function operatorFormat(operator, port) {
    return templates.phys_op_link({ appId: operator.get('appId'), operatorId: operator.get('id'), displayText: operator.get('name') + ' (' + operator.get('id') + ')' });
}

function windowFormat(operator, port) {
    return formatters.windowFormatter(operator.get('currentWindowId'));
}

exports = module.exports = function(portType) {
    return [
        // portName
        { id: 'portName', key: 'name', label: 'Port Name', sort: 'string', filter: 'like', sort: 'string', sort_value: 'a', format: portFormat },
        // operator
        { id: 'operator', key: 'operator', label: 'Operator Name (ID)', filter: 'likeFormatted', sort: 'string', format: operatorFormat },
        // bufferServerBytesPSMA10
        { id: 'bufferServerBytesPSMA', key: 'bufferServerBytesPSMA', label: 'Buffer ' + ( portType === 'source' ? 'Writes' : 'Reads' ) + '/sec' , filter: 'number', sort: 'number', format: 'commaInt' },
        // tuplesPSMA10
        { id: 'tuplesPSMA', key: 'tuplesPSMA', label: 'Tuples/sec', filter: 'number', sort: 'number', format: 'commaInt' },
        // totalTuples
        { id: 'totalTuples', key: 'totalTuples', label: 'Total Tuples', filter: 'number', sort: 'number', format: 'commaInt' },
        // current window
        { id: 'currentWindow', key: 'operator', label: 'Current Window', filter: 'likeFormatted', format: windowFormat }
    ]
}