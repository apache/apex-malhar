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
var _ = require('underscore'), Backbone = require('backbone');
var Notifier = require('./Notifier');
var BaseCollection = require('./BaseCollection');
var App = require('./ApplicationModel');

var ApplicationCollection = BaseCollection.extend({

    debugName: 'applications',
    
    model: App,
    
    url: function() {
        return this.resourceURL('Application');
    },
    
    responseTransform: 'apps',
    
    initialize: function(models, options) {
        // Set up dataSource
        this.dataSource = options.dataSource;
    },
    
    setRunning: function(apps, options) {
        // count all that are running
        var beforeCount = this.where({state: 'RUNNING'}).length;
        var nowCount = _.where(apps, {state: 'RUNNING'}).length;
        
        // Check if an app has been killed.
        if (nowCount < beforeCount) {
            // If so, we have to retrieve all apps
            this.fetch();
        }
        
        // Set new rows
        this.set(apps, options);
    },
    
    subscribe: function() {
        // Set up subscription
        
        var topic = this.resourceTopic('Applications');
        
        this.listenTo(this.dataSource, topic, function(data){
            var rows = data.apps;
            this.setRunning(rows,{remove: false});
        });
        this.dataSource.subscribe(topic);
    }
    
});

exports = module.exports = ApplicationCollection;