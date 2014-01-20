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
/*

Tuple Viewer state model

This model will hold the state for the tupleviewer widget. It is an extension
of the RecordingModel. The following attributes are added for keeping track of the
widget's state:

    offset  - BigRat, the tuple offset
    limit   - Int, the number of tuples to show in the list
    currentWindow
    
Additionally, two collections will be added to this model:

    ports   - Holds ports for the recording.
    tuples  - Holds loaded tuples. Should use an LRU cache scheme.
    
*/
var _ = require('underscore');
var bigRat = require('big-rational');
var BigInteger = require('jsbn');
var Recording = DT.lib.RecordingModel;
var WindowId = DT.lib.WindowId;
var ViewerRecording = Recording.extend({
    
    defaults: {
        appId: '',
        ended: undefined,
        operatorId: '',
        ports: [],
        recordingName: '',
        startTime: '',
        totalTuples: '0',         // br
        windowIdRanges: [],
        offset: '0',              // br
        limit: 20,
        currentWindowId: '',      // bi
        currentTupleIndex: '0',   // bi
        notfound: false,
        innerTupleHeight: 20,
        tuplePaddingTB: 3,
        tupleMarginBtm: 5,
        scrollerWidth: 50
    },
    
    initialize: function(attrs, options) {
        // super (defines this.ports)
        Recording.prototype.initialize.call(this, attrs, options);
        
        this.on('change:offset change:totalTuples', function(){
            this.makeTupleRequest();
        }, this);
        
        this.on('change', function(){
            this.__not_changed__ = false;
        }, this);
        
        this.listenTo(this.ports, "update change:selected", function() {
            // clear all tuples
            this.tuples.reset([]);
            // set offset to 0
            this.set({'offset':'0'},{silent:true});
            this.trigger('change:offset');
        });
        
        this.listenTo(this.tuples, 'change:selected', function() {
            var selectedTuples = this.tuples.where({"selected":true});
            var curTuple = selectedTuples.length ? selectedTuples[0] : false ;
            this.set('currentTupleIndex', curTuple ? curTuple.get("idx") : '-' );
            this.set('currentWindowId', curTuple ? curTuple.get("windowId") : '-1' );
        });
    },
    
    serialize: function(nocache) {
        
        if (!nocache && this.__cached_json__ && this.__not_changed__) {
            return this.__cached_json__;
        }
        
        var json = this.toJSON();
        
        // tuple index
        if (!json.currentTupleIndex) {
            json.currentTupleIndex = '-';
        }
        
        // currentTotal
        json.currentTotal = this.getCurrentTotal().toString();
        
        // status of this recording
        json.loadedStatus = json.startTime ? 'loaded' : ( json.notfound ? 'not found' : 'loading' );
        
        // add calculated height for tuples and for widget
        json.tupleHeight = this.getTupleHeight();
        json.scrollerHeight = json.tupleHeight * json.limit;
        if (json.currentTotal > 0) {
            json.scrollViewportHeight = bigRat(json.limit, json.currentTotal).times(json.scrollerHeight).toDecimal(1);     // vph = (limit / totalTuples) * scrollerHeight
            json.scrollViewportTop = bigRat(json.offset, json.currentTotal).times(json.scrollerHeight).toDecimal(1);
        }
        else {
            json.scrollViewportHeight = '0';
            json.scrollViewportTop = '0';
        }
        
        this.__cached_json__ = json;
        this.__not_changed__ = true;
        
        return json;
    },
    
    getCurrentTotal: function() {
        return this.ports.reduce(function(memo, val){
            if (val.get('selected')) return memo.add(new BigInteger(val.get('tupleCount')+''));
            else return memo;
        }, BigInteger.ZERO );
    },
    
    goToTupleAtWindow: function(windowId) {
        this.waitingForWindowId = windowId;
        this.makeTupleRequest(windowId);
    },
    
    makeTupleRequest: function(windowId) {
        var offset = this.get('offset'), limit = this.get('limit');
        return this.requestTuples( offset, limit, windowId );
    },
    
    waitingForWindowId: false,
    
    shiftSelected: function(change) {
        var currentTotal = this.getCurrentTotal();
        var change = new BigInteger(change+'');
        var selected = this.tuples.where({"selected":true});
        if (!selected.length) return;
        
        // remove all selected that are outside viewing range
        // set view range (inclusive)
        var offset = new BigInteger(this.get("offset")+'');
        var limit = new BigInteger(this.get("limit")+'');
        var upper = offset.add(limit).subtract(BigInteger.ONE);
        // filter out
        selected = selected.filter(function(tuple){
            var this_index = new BigInteger(tuple.get("idx")+'');
            return this_index.compareTo(offset) >= 0 && this_index.compareTo(upper) <= 0 ;
        });
        // Shifting down
        if (change.compareTo(BigInteger.ZERO) > 0) {
            var lastIdx = offset.add(limit).subtract(BigInteger.ONE);
            var tuple = this.tuples.get(lastIdx.toString());
            if (tuple && tuple.get("selected")) {
                this.set({"offset":offset.add(change).toString()},{validate: true});
            }
        } 
        // Shifting up
        else {
            var firstIdx = offset;
            var tuple = this.tuples.get(firstIdx.toString());
            if (tuple && tuple.get("selected")) {
                this.set({"offset":offset.add(change).toString()},{validate: true});
            }
        }
        

        var anchor_idx = new BigInteger(this.tuples.where({"anchored":true})[0].get("idx")).add(change);
        var anchored = false;
        var last = undefined;
        var indeces = _.map(selected, function(tuple) { return tuple.get("idx") });
        this.tuples.deselectAll();
        indeces.forEach(function(val){
            // var newIdx = Math.min(self.get("currentTupleCount") - 1, Math.max(val*1 + change, 0));
            var newIdx = currentTotal.subtract(BigInteger.ONE).min((new BigInteger(val+'')).add(change).max(BigInteger.ZERO));
            var next = this.tuples.get(newIdx.toString());
            if (next) {
                var data = {"selected":true};
                if (newIdx == anchor_idx) {
                    data.anchored = true;
                    anchored = true;
                }
                next.set(data);
                last = next;
            }
        }, this);
        if (!anchored && last) {
            last.set("anchored", true);
        }
    },
    
    stepToWindow: function(step) {
        
        // Check if there is even a selected tuple to go from
        var anchor = this.tuples.where({"selected":true,"anchored":true})[0];
        if (anchor === undefined) return;
        var ranges = this.get("windowIdRanges");
        var curWindowId = anchor.get("windowId");
        var BI_windowId = new BigInteger(curWindowId+'');
        
        // going back
        if (step < 0) {
            
            // iterator for moving windows
            var k = -1;
            while (k >= step) {
                // look for lows
                var jumped = false;
                for (var i=0; i < ranges.length; i++) {
                    var prevRange = ranges[i-1];
                    var range = ranges[i];
                    if (range.low === curWindowId) {
                        if (prevRange === undefined) return;
                        curWindowId = prevRange.high;
                        BI_windowId = new BigInteger(prevRange.high);
                        jumped = true;
                    }
                };
                if (!jumped) {
                    // no jump between ranges made, subtract one
                    BI_windowId = BI_windowId.subtract(BigInteger.ONE);
                    curWindowId = BI_windowId.toString();
                }
                // check if tuples are available for this window
                k--;
            }
            
        }
        else if (step > 0) {
            
            // iterator for moving windows
            var k = 1;
            while (k <= step) {
                // look for lows
                var jumped = false;
                for (var i=0; i < ranges.length; i++) {
                    var nextRange = ranges[i+1];
                    var range = ranges[i];
                    if (range.high === curWindowId) {
                        if (nextRange === undefined) return;
                        
                        curWindowId = nextRange.low;
                        BI_windowId = new BigInteger(nextRange.low);
                        jumped = true;
                        console.log("jump!", range);
                    }
                };
                if (!jumped) {
                    // no jump between ranges made, subtract one
                    BI_windowId = BI_windowId.add(BigInteger.ONE);
                    curWindowId = BI_windowId.toString();
                }
                // check if tuples are available for this window
                k++;
            }
        }
        // deselect all 
        this.tuples.deselectAll(true);
        
        this.goToTupleAtWindow(curWindowId);
    },
    
    getTupleHeight: function() {
        return this.get("innerTupleHeight") + this.get("tupleMarginBtm") + 2 * this.get("tuplePaddingTB");
    }
    
});
exports = module.exports = ViewerRecording;