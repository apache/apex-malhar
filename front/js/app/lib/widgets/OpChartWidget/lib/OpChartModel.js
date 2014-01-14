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
 * Chart Recording Model
 * 
 * Extends the original recording model
*/
var BigInteger = require('jsbn');
var util = require('./util');
var Recording = DT.lib.RecordingModel;
var ChartRecording = Recording.extend({
    
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
        limit: 50,
        min_limit: 1,
        max_points: 50,
        tail: true, // true if the viewport should follow incoming data
        scroller_height: 70,
        scroller_width: 0
    },
    
    initialize: function(attrs, options) {
        Recording.prototype.initialize.call(this, attrs, options);
        
        // Store ref to itself for comparator
        var self = this;
        
        // Set up comparator for tuples
        this.tuples.comparator = function(row1, row2) {
            return self.tupleComparator(row1, row2);
        }
        
        // Listen for change in offset
        this.on('change:offset', this.makeTupleRequest);
        
        this.on('change:limit', function(model, newLimit) {
            this.adjustOffsetForTail(newLimit, false);
        });
        
        this.on('change:tail', function(model, tail) {
            if (tail) {
                this.adjustOffsetForTail();
            }
        });
        
        // Listen for changes in totaltuples
        this.on('change:totalTuples', function(model, newTotal) {
            this.adjustOffsetForTail(false, newTotal);
        });
    },
    
    validate: function(attrs) {
        var offset = new BigInteger(attrs.offset+'');
        var total = typeof this.getCurrentTotal === 'function' ? this.getCurrentTotal() : new BigInteger(this.get('totalTuples')+'') ;
        var upper = total.subtract(new BigInteger(attrs.limit+''));
        if (offset.compareTo(BigInteger.ZERO) < 0) return 'The offset cannot be less than zero.';
        if (attrs.limit*1 > 300) return 'Limit cannot exceed 300.';
    },
    
    adjustOffsetForTail: function(newLimit, newTotal) {
        if (this.get('tail')) {
            // calculate new offset
            var limit = new BigInteger(newLimit ? newLimit+'' : this.get('limit')+'');
            var total = new BigInteger(newTotal ? newTotal+'' : this.get('totalTuples')+'');
            var newOffset = total.subtract(limit);
            if (newOffset.compareTo(BigInteger.ZERO) < 0) {
                newOffset = BigInteger.ZERO;
            }
            this.set('offset', newOffset.toString());
        }
    },
    
    serialize: function() {
        var json = this.toJSON();
        
        // calculate viewport dims
        json.viewport_left = util.scaleFraction(json.offset, json.totalTuples, json.scroller_width);
        // json.viewport_width = util.scaleFraction(json.limit, json.totalTuples, json.scroller_width);
        json.viewport_width = 1;
        json.viewport_min_limit = util.scaleFraction(json.min_limit, json.totalTuples, json.scroller_width);
        
        return json;
    },
    
    tupleComparator: function(row1, row2) {
        // return new BigInteger(row1.get('idx')+'').compareTo(new BigInteger(row1.get('idx')+''));
        return 0;
    },
    
    updateWith: function(attrs) {
        // Update the model
        Recording.prototype.set.call(this, attrs);
        
        // Chart-type-specific setup
        switch(attrs.properties.chartType) {
            case 'LINE':
                // set the comparator
                this.tupleComparator = function(row1, row2) {
                    var x1 = new BigInteger(row1.get('x_key')+'');
                    var x2 = new BigInteger(row2.get('x_key')+'');
                    return x1.compareTo(x2);
                }
                
                // Get the initial tuples
                this.makeTupleRequest();
                
            break;
            
            case 'CANDLE':
            
            break;
        }

        this.subscribe();
    },
    
    makeTupleRequest: function() {
        var offset = this.get('offset'), limit = this.get('limit');
        return this.requestTuples( offset, limit );
    },
    
    onLiveTuples: function(tuple) {
        
        // get the new totalTuples value
        var newTotal = tuple.tupleCount;
        
        // transform the tuple data
        tuple = util.transformLiveTuple(tuple);

        // update tuple collection
        this.tuples.set([tuple], {remove: false});
        
        // update the totalTuples
        this.set('totalTuples', newTotal);
        
    },
    
    createViewportOptions: function(id) {
        var props = this.get('properties');
        if (!props.hasOwnProperty('chartType')) throw new Error('This recording cannot be charted.');
        
        var options = {
            vp_width: 700,
            vp_height: 400
        };
        
        if (id) options.id = id;
        
        switch(props.chartType) {
            case 'LINE':
                options.time_key = 'x_key';
                options.data = this.tuples;
            break;
            
            case 'CANDLE':
                options['x_key'] = 'x_key';
                options['multi_y'] = true;
                options['type'] = 'candle';
            break;
            
            default:
                throw new Error('Chart type "'+props.chartType+'" not currently supported');
            break;
        }
        
        return options;
    },
    
    setChart: function(chart) {
        if (this._chart) {
            this.stopListening(this._chart.model);
        }
        this._chart = chart;
        this.listenTo( this._chart.model, 'change:vp_width', function(model, value) {
            this.set('scroller_width', value);
        });
        this.set('scroller_width', this._chart.model.get('vp_width'));
    }
    
});
exports = module.exports = ChartRecording;