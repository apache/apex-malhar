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
 * Column definitions for application instance list
*/
var bormat = require('bormat');
var templates = DT.templates;
var formatters = DT.formatters;
var stateOrder = DT.settings.statusOrder;

function stateFormatter(value,row) {
    if (!value) {
        return '-';
    }
    var finalStatus = row.get('finalStatus');
    var html = '<span class="status-' + value.replace(' ','-').toLowerCase() + '">' + value + '</span>';
    if ( typeof finalStatus === 'string' && finalStatus.toLowerCase() !== 'undefined' ) {
        html += ' <span class="final-status" title="Final Status">(' + finalStatus + ')</span>';
    }
    return html;
}

function stateSorter(row1,row2) {
    var state1 = stateOrder.indexOf(row1.get('state'));
    var state2 = stateOrder.indexOf(row2.get('state'));
    return state1 - state2;
}

function idFormatter(value, row) {
    return templates.app_instance_link({ appId: value });
}

function idFilter(term, value, computedValue, row) {
    var parts = value.split('_');
    term = term.toLowerCase();
    value = parts[parts.length-1]+'';
    value = value.toLowerCase();
    return value.indexOf(term) > -1;
}

function lifetimeFormatter(value, row) {
    var finishedTime = row.get('finishedTime') * 1 || +new Date() ;
    var startedTime = row.get('startedTime') * 1 ;
    return bormat.timeSince({
        timeChunk: finishedTime - startedTime,
        unixUptime: true,
        max_levels: 3
    });
}

function memoryFormatter(value, row) {
    if (!value) {
        return '-';
    }
    return formatters.byteFormatter(value, 'mb');
}

function memorySorter(row1, row2) {
    var v1 = row1.get('allocatedMB');
    var v2 = row2.get('allocatedMB');
    if (!v1 && !v2) {
        return 0;
    }
    if (!v1) {
        return -1;
    }
    if (!v2) {
        return 1;
    }
    return v1 - v2;
}

exports = module.exports = [
    { id: 'selector', key: 'selected', label: '', select: true, width: 40, lock_width: true },
    { id: 'id', label: DT.text('id_label'), key: 'id', sort: 'string', filter: idFilter, format: idFormatter, width: 50, sort_value: 'd', lock_width: true },
    { id: 'name', key: 'name', label: DT.text('name_label'), sort: 'string', filter: 'like', width: 200 },
    { id: 'state', label: DT.text('state_label'), key: 'state', format: stateFormatter, sort: stateSorter, filter:'like', width: 100 , sort_value: 'a'},
    { id: 'user', key: 'user', label: DT.text('user_label'), sort: 'string', filter:'like' },
    { id: 'startedTime', label: DT.text('started_label'), key: 'startedTime', sort: 'number', filter: 'date', format: 'timeSince' },
    { id: 'lifetime', label: DT.text('lifetime_label'), key: 'startedTime', filter: 'numberFormatted', format: lifetimeFormatter },
    { id: 'allocatedMB', label: DT.text('memory_label'), key: 'allocatedMB', sort: memorySorter, filter: 'number', format: memoryFormatter, width: 60 }
];