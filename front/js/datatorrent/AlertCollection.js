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
var Backbone = require('backbone');
var Notifier = require('./Notifier');
var BaseCollection = require('./BaseCollection');
var AlertModel = require('./AlertModel');
var path = require('path');
var Notifier = require('./Notifier');
var settings = require('./settings');

var AlertCollection = BaseCollection.extend({

    debugName: 'alerts',

    model: AlertModel,
    
    responseTransform: 'alerts',
    
    appId: null,

    initialize: function(attributes, options) {
        BaseCollection.prototype.initialize.call(this, attributes, options);
        this.appId = options.appId;
    },

    url: function() {
        return this.resourceURL('Alert', {
            appId: this.appId
        });
    },

    fetch: function(options) {
        options = options || {};
        options.reset = true;
        BaseCollection.prototype.fetch.call(this, options);
    },
    
    fetchError: function() {
        Notifier.error({
            title: 'Could not get alerts',
            text: 'An error occurred retrieving the alerts for ' + this.appId + '.'
        });
    }
});

exports = module.exports = AlertCollection;