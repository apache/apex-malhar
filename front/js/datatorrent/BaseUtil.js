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
    Backbone   = require('backbone'),
    Notifier   = require('./Notifier');
    
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
    Notifier.error(message);
}

function quietFetchError(object, response, options) {
    if (typeof LOG === 'function') {
        LOG(4, this.debugName + ' failed to load (' + response.status + ')', ['Server responded with: ' + response.statusText]);
    }
}

function responseFormatError() {
    var message = {
        'title': this.debugName + ' not in expected format',
        'text': 'The response returned by the daemon was not in the expected format.'
    };
    Notifier.error(message);
}

exports.resourceURL = makeResourceFn('urls');
exports.resourceTopic = makeResourceFn('topics');
exports.resourceAction = makeResourceFn('actions');
exports.subscribeToTopic = subscribeToTopic;
exports.fetchError = fetchError;
exports.quietFetchError = quietFetchError;
exports.responseFormatError = responseFormatError;