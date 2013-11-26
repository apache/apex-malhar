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
var _          = require('underscore'),
    Backbone   = require('backbone');
    
function makeResourceFn(settingAttr) {
    return function(path, params) {
        params = _.extend(params || {}, { v: this.settings.version });
        var string = this.settings[settingAttr][path];
        if (string === undefined) {
            throw new Error('\'' + path + '\' was not found in settings.' + settingAttr);
        }
        return this.settings.interpolateParams(string, params);
    };
}

function resourceAction(path, params) {
    params = params || {};
    var actionString = this.settings.actions[path];
    if (actionString === undefined) {
        throw new Error('The action named \'' + path + '\' was not found in settings.actions!');
    }
    return this.settings.interpolateParams(actionString, params);
}

function subscribeToTopic(topic) {
    this.checkForDataSource();
    this.listenTo(this.dataSource, topic, this.set);
    this.dataSource.subscribe(topic);
}

function fetchError(object, response, options) {
    var message = {
        'title': this.debugName + ' failed to load (' + response.status + ')',
        'text': 'Server responded with: ' + response.statusText
    };
    return message;
}

function responseFormatError() {
    var message = {
        'title': this.debugName + ' not in expected format',
        'text': 'The response returned by the daemon was not in the expected format.'
    };
    return message;
}

exports.resourceURL = makeResourceFn('urls');
exports.resourceTopic = makeResourceFn('topics');
exports.resourceAction = makeResourceFn('actions');
exports.subscribeToTopic = subscribeToTopic;
exports.fetchError = fetchError;
exports.responseFormatError = responseFormatError;