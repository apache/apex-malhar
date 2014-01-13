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
 * Base model for recordings
*/
var _ = require('underscore');
var Notifier = require('./Notifier');
var BigInteger = require('jsbn');
var Ports = require('./PortCollection');
var Tuples = require('./TupleCollection');
var BaseModel = require('./BaseModel');
var RecordingModel = BaseModel.extend({
    
    debugName: 'RecordingModel',
    
    idAttribute: 'startTime',
    
    defaults: {
        appId: '',
        ended: undefined,
        operatorId: '',
        ports: [],
        recordingName: '',
        startTime: '',
        totalTuples: '0',
        windowIdRanges: [],
        properties: {},
        offset: '0',
        limit: 20
    },
    
    constructor: function() {
        this.ports = new Ports([], {} );
        BaseModel.prototype.constructor.apply(this, _.toArray(arguments));
    },
    
    initialize: function(attrs, options) {
        
        // Ensure options and attrs are objects
        attrs = attrs || {};
        options = options || {};
        
        // Check for dataSource
        if (options.dataSource) {
            this.dataSource = options.dataSource;
        }
        
        // tuples collection
        this.tuples = new Tuples([]);
    },
    
    set: function(attrs, options) {
        
        if (typeof attrs !== 'object') {
            var tmp = {};
            tmp[attrs] = options;
            attrs = tmp;
            options = {};
        }
        
        if (attrs.ports) {
            this.ports.set(attrs.ports);
            delete attrs.ports;
        }
        
        BaseModel.prototype.set.call(this, attrs, options);
    },
    
    urlRoot: function() {
        return this.resourceURL('Recording', {
            appId: this.get('appId'),
            operatorId: this.get('operatorId')
        });
    },
    
    validate: function(attrs) {
        var offset = new BigInteger(attrs.offset+'');
        var total = typeof this.getCurrentTotal === 'function' ? this.getCurrentTotal() : new BigInteger(this.get('totalTuples')+'') ;
        var upper = total.subtract(new BigInteger(attrs.limit+''));
        if (offset.compareTo(BigInteger.ZERO) < 0) return 'The offset cannot be less than zero.';
        if (offset.compareTo(upper) > 0) return 'The offset cannot exceed total.';
    },
    
    requestTuples: function(offset, limit, windowId) {
        // Use a timeout to prevent over-polling 
        // the server for tuples
        if (this.__tupleReqTO__) clearTimeout(this.__tupleReqTO__);
        this.__tupleReqTO__ = setTimeout(function(offset, limit, windowId){
            
            // Get selected ports
            var selected_ports = _.map(this.ports.where({ selected: true }), function(port) {
                return port.get('id');
            });
            if (!selected_ports.length) {
                selected_ports = this.ports.map(function(port) {
                    return port.get('id');
                });
            }
            
            // Create the request object
            var reqObject = {
                appId: this.get('appId'),
                operatorId: this.get('operatorId'),
                startTime: this.get('startTime'),
                ports: selected_ports
            };
            offset = new BigInteger(offset+'');
            limit = new BigInteger(limit+'');
            var actualLimit = new BigInteger(limit+'');
            var actualOffset = new BigInteger(offset+'');
            
            if (!windowId){
                
                // check which outer tuples have already been loaded
                for(var i = BigInteger.ZERO; i.compareTo(limit) < 0; i = i.add(BigInteger.ONE)) {
                    var checkIdx = i.add(offset).toString();
                    var tuple = this.tuples.get(checkIdx);
                    if ( tuple === undefined) break;
                    if ( tuple.get('portId') === '' ) break;
                    actualOffset = actualOffset.add(BigInteger.ONE);
                    actualLimit = actualLimit.subtract(BigInteger.ONE);
                }
                reqObject.offset = actualOffset.toString();
                
            } else {
                
                reqObject.startWindow = windowId;
                
            }
            
            reqObject.limit = actualLimit.toString();
            
            if (actualLimit.compareTo(BigInteger.ZERO) <= 0) {
                // The limit was zero or less, don't do anything.
                return;
            }
            
            reqObject.success = function(res) {
                this.handleTuples(res);
                LOG(2, 'tuples received from getRecordingTuples', [res]);
            }.bind(this);

            reqObject.error = function(res) {
                LOG(4, 'An error occurred with getRecordingTuples', ['reqObj: ', reqObject, 'res: ', res]);
            };

            this.dataSource.getRecordingTuples(reqObject);
            
        }.bind(this, offset, limit, windowId), 200);
    },
    
    handleTuples: function(data){
        var retTuples = [];
        var idx = data.startOffset;
        var idxBR = new BigInteger(data.startOffset+'');
        
        // check for get offset
        if (data.tuples.length && data.tuples[0].windowId == this.waitingForWindowId) {
            var upper = this.getCurrentTotal().subtract(new BigInteger(this.get('limit')+''));
            this.set({'offset': idxBR.min(upper).max(BigInteger.ZERO).toString()}, {validate: true});
            this.waitingForWindowId = false;
            // set new selected
            var newSelected = this.tuples.get(idx);
            newSelected.set({'selected':true, 'anchored':true});
            newSelected.once('change', function() {
                this.set('currentWindowId', newSelected.get('windowId'));
                this.trigger('update_console');
            }, this);
        }
        
        // load tuple data from groups
        for (var i=0; i < data.tuples.length; i++) {
            var tplGrp = data.tuples[i];
            var windowId = tplGrp.windowId;
            var tuples = tplGrp.tuples;
            tuples.forEach(function(tuple){
                tuple.idx = idxBR.toString();
                tuple.windowId = windowId;
                retTuples.push(tuple);
                idxBR = idxBR.add(BigInteger.ONE);
            });
        }
        
        
        this.tuples.set(retTuples,{remove: false});
        this.tuples.trigger('update');
    },
    
    subscribe: function() {

        var startTime = this.get('startTime');
        var ended = !! this.get('ended');
        if (ended) {
            LOG(3, 'Cannot subscribe to a recording that has ended.');
            return;
        }
        if (!startTime) {
            LOG(3, 'The recording does not have a startTime');
            return;
        }
        var topic = this.resourceTopic('TupleRecorder', {
            startTime: startTime
        });
        
        this.listenTo(this.dataSource, topic, this.onLiveTuples);
        this.dataSource.subscribe(topic);
    },
    
    unsubscribe: function() {
        var topic = this.resourceTopic('TupleRecorder', {
            startTime: startTime
        });
        this.stopListening(this.dataSource, topic);
    },
    
    // Dummy function, should be overridden by child class
    onLiveTuples: function(data) {
        LOG(3, 'onLiveTuples needs to be implemented in a subclass. Currently doing nothing.');
    }
    
});
exports = module.exports = RecordingModel;