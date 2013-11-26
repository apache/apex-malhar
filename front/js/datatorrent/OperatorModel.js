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
 * Operator model
*/

var _ = require('underscore'), Backbone = require('backbone');
var bormat = require('bormat');
var WindowId = require('./WindowId');
var formatters = require('./formatters');
var BaseModel = require('./BaseModel');
var OperatorModel = BaseModel.extend({
    
    debugName: 'operator',
    
    defaults: {
        'className': '',
        'container': '',
        'cpuPercentageMA': '',
        'currentWindowId': '',
        'recoveryWindowId': '',
        'recoveryWindowId_f': new WindowId('0'),
        'currentWindowId_f': new WindowId('0'),
        'failureCount': '',
        'host': '',
        'id': '',
        'ports': [],
        'lastHeartbeat': '',
        'latencyMA': '',
        'name': '',
        'recordingStartTime': '-1',
        'status': '',
        'totalTuplesEmitted': '',
        'totalTuplesProcessed': '',
        'tuplesEmittedPSMA': '',
        'tuplesProcessedPSMA': ''
    },
    
    serialize: function(noFormat) {
        var obj = this.toJSON();
        
        if ( !noFormat ) {
            // Update WindowIds
            _.each(['recoveryWindowId', 'currentWindowId'], function(key) {
                obj[key + '_f'].set(obj[key]);
            }, this);
            
            // Make comma group formatting
            _.each(['totalTuplesEmitted','tuplesEmittedPSMA','totalTuplesProcessed','tuplesProcessedPSMA'], function(key){
                obj[key + '_f'] = bormat.commaGroups(obj[key]);
            });
            
        }
        return obj;
    },
    
    urlRoot: function() {
        var urlRoot = this.resourceURL('Operator', {
            appId: this.get('appId')
        });
        return urlRoot;
    },
    
    subscribe: function() {
        var topic = this.resourceTopic('Operators', {
            appId: this.get('appId')
        });
        this.checkForDataSource();
        this.listenTo(this.dataSource, topic, function(data) {
            var operatorId = this.get('id');
            var obj = _.find(data.operators, function(op) {
                return op.id == operatorId;
            });
            obj.appId = this.get('appId');
            this.set(obj);
            this.trigger('update');
        });
        this.dataSource.subscribe(topic);
    }
    
});
exports = module.exports = OperatorModel;