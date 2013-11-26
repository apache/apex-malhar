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
var stateOrder = ['RUNNING','FAILED','FINISHED','KILLED'];

function stateFormatter(value,row) {
    if (value == null || value === "")
        return "-";
    return '<span class="status-' + value.replace(' ','-').toLowerCase() + '">' + value + '</span> <span class="final-status" title="Final Status">(' + row.get('finalStatus') + ')</span>';
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

function finalStatusFormatter(value, row) {
    return value;
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

exports = module.exports = [
    { id: "selector", key: "selected", label: "", select: true, width: 40, lock_width: true },
    { id: "id", label: "App Id", key: "id", sort: "string", filter: idFilter, format: idFormatter, width: 70, sort_value: "d" },
    { id: "name", key: "name", label: "App Name", sort: "string", filter: "like", width: 200 },
    { id: "state", label: "State", key: "state", format: stateFormatter, sort: stateSorter, filter:"like", width: 100 , sort_value: "a"},
    { id: "user", label: "User", key: "user", sort: "string", filter:"like" },
    { id: "startedTime", label: "Started", key: "startedTime", sort: "number", filter: "date", format: "timeSince" },
    { id: "lifetime", label: "Lifetime of App", key: "startedTime", filter: "numberFormatted", format: lifetimeFormatter }
]