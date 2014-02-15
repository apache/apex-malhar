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
 * Container Collection
*/
var Base = require('./ApplicationSubCollection');
var Container = require('./ContainerModel');
var ContainerCollection = Base.extend({
    
    debugName: 'containers',
    
    responseTransform: 'containers',
    
    model: Container,

    initialize: function() {
        Base.prototype.initialize.apply(this, arguments);
        this.on('remove', function(model) {
            console.log('removal from container collection');
        });
    },
    
    url: function() {
        return this.resourceURL('Container', {
            appId: this.appId
        });
    },
    
    subscribe: function() {
        var topic = this.resourceTopic('Containers', {
            appId: this.appId
        });
        this.listenTo(this.dataSource, topic, function(data) {
            this.set(data.containers);
        });
        this.dataSource.subscribe(topic);
    }
        
});
exports = module.exports = ContainerCollection;