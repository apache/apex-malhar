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
 * Port model.
*/

var _ = require('underscore');
var bormat = require('bormat');
var BaseModel = require('./BaseModel');
var PortModel = BaseModel.extend({

    debugName: 'port',
    
    idAttribute: 'name',
    
    defaults: {
        appId: undefined,
        operatorId: undefined,

        bufferServerBytesPSMA: '',
        name: '',
        totalTuples: '',
        tuplesPSMA: '',
        type: '',
        selected: true
    },

    urlRoot: function() {
        return this.resourceURL('Port', {
            appid: this.get('appId'),
            operatorId: this.get('operatorId')
        });
    },

    serialize: function(noFormat) {
        var obj = this.toJSON();
        
        if (!noFormat) {
            _.each(['bufferServerBytesPSMA', 'tuplesPSMA', 'totalTuples'], function(key) {
                obj[key + '_f'] = bormat.commaGroups(obj[key]);
            });
        }
        
        return obj;
    },

    subscribe: function() {

        var topic = this.resourceTopic('Operators', {
            appId: this.get('appId')
        });
        var operatorId = this.get('operatorId');
        var portName = this.get('name');
        
        this.checkForDataSource();
        this.listenTo(this.dataSource, topic, function(res) {
            
            var operator = _.find(res.operators, function(op) {
                return op.id == operatorId;
            });

            var port = _.find(operator.ports, function(p) {
                return p.name == portName;
            });

            this.set(port);
            this.trigger('update');
        });

        this.dataSource.subscribe(topic);
    }
    
});

exports = module.exports = PortModel;