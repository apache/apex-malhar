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
 * Operator Collection
*/
var _ = require('underscore');
var Base = require('./ApplicationSubCollection');
var Operator = require('./OperatorModel');
var OperatorCollection = Base.extend({
    
    debugName: 'operators',
    
    model: Operator,
    
    initialize: function(models, options) {
        Base.prototype.initialize.call(this, models, options);
        this.containerId = options.containerId || false; 
    },
    
    toJSON: function(aggregates) {
        var result = [];
        this.each(function(op) {
            result.push(op.serialize(true, aggregates));
        });
        return result;
    },
    
    serialize: function(aggregates) {
        var result = [];
        this.each(function(op) {
            result.push(op.serialize(false, aggregates));
        });
        return result;
    },
    
    url: function() {
        return this.resourceURL('Operator', {
            appId: this.appId
        });
    },
    
    subscribe: function() {
        var topic = this.resourceTopic('Operators', {
            appId: this.appId
        });
        this.listenTo(this.dataSource, topic, function(data) {
            this.set(data.operators);
        });
        this.dataSource.subscribe(topic);
    },

    unsubscribe: function() {
        var topic = this.resourceTopic('Operators', {
            appId: this.appId
        });
        this.stopListening(this.dataSource, topic);
    },
    
    set: function(models, options) {
        
        var containerId = this.containerId;
        
        if ( containerId ) {
            models = _.filter(models, function(op) {
                return op.container === containerId;
            });
        }
        
        Base.prototype.set.call(this, models, options);
    },
    
    responseTransform: 'operators'
    
});
exports = module.exports = OperatorCollection;