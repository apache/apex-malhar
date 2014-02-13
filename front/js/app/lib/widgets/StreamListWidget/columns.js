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
var templates = DT.templates;

function nameFormatter(name, model) {
    return templates.stream_name_link({ appId: model.get('appId'), streamName: name });
}

function sourceFormatter(source) {
    return source.operatorName + ': ' + source.portName;
}

function sinksFormatter(sinks) {
    var sinkStrings = _.map(sinks, function(sink) {
        return sink.operatorName + ': ' + sink.portName;
    });
    return sinkStrings.join(', ');
}
function localityFormatter(locality, row) {
    return locality || DT.text('locality_not_assigned');
}

exports = module.exports = [
    { id: 'selector', label: '', key: 'selected', select: true, width: 40, lock_width: true },
    { id: 'name', label: DT.text('name_label'), key: 'name', filter: 'like', sort: 'string', sort_value: 'a', format: nameFormatter },
    { id: 'locality', label: DT.text('locality_label'), key: 'locality', filter: 'like', sort: 'string', format: localityFormatter },
    { id: 'source', label: DT.text('source_label'), key: 'source', filter: 'likeFormatted', format: sourceFormatter },
    { id: 'sinks', label: DT.text('sinks_label'), key: 'sinks', filter: 'likeFormatted', format: sinksFormatter }
];