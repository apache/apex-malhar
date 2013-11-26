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
 * Overview for OpChart
 * 
 * This component allows the user to
 * navigate a recording at full length.
*/
var _ = require('underscore');
var util = require('./util');
var Backbone = require('backbone');
var kt = require('knights-templar');
var Chart = DT.lib.LiveChart;
var BaseView = require('bassview');
var Overview = BaseView.extend({
    
    initialize: function(options) {
        
        // Get the viewport chart object
        this.vChart = options.vpChart;
        
        this.listenTo(this.model, 'change:scroller_width', this.render);
        this.listenTo(this.model, 'change:offset change:limit', this.setViewport);
        this.listenTo(this.vChart.model, 'yaxes_rendered', this.adjustLeftMargin)
    },
    
    render: function() {
        var marginLeft = this.vChart.view.$('.chart-yaxes').width();
        var scroller_width = this.vChart.model.get('vp_width');
        var json = this.model.serialize();
        var markup = this.template(json);
        this.$el
        .html(markup)
        .css('marginLeft',marginLeft+'px');
        
        // save the viewport object
        this.$vp = this.$('.viewport');
        return this;
    },
    
    adjustLeftMargin: function() {
        this.$el.css('marginLeft', this.vChart.view.$('.chart-yaxes').width()+'px');
    },
    
    setViewport: function() {
        var json = this.model.serialize();
        this.$vp.css({
            'left': json.viewport_left+'px',
            'width': json.viewport_width+'px'
        });
    },
    
    events: {
        'mousedown .interaction': 'onMouseDown',
        'mousedown .right-handle': 'grabRightHandle',
        'mousedown .left-handle': 'grabLeftHandle'
        
    },
    
    onMouseDown: function(e) {
        this.model.set('tail', false);
        this.setNewOffset(e);
        this.grabViewport(e);
    },
    
    setNewOffset: function(e) {
        
        // Get recording infor
        var json = this.model.serialize();
        
        // Calculate the mouse offset
        var mouseOffset = e.clientX - this.$el.offset().left + window.pageXOffset;
        mouseOffset = Math.min( json.scroller_width, mouseOffset);
        
        // Calculate the actual offset
        var recOffset = util.scaleFraction(mouseOffset, json.scroller_width, json.totalTuples, true);
        this.model.set({'offset': recOffset},{validate: true});
    },
    
    grabViewport: function(e) {
        if (e) {
            e.stopPropagation();
            e.originalEvent.preventDefault();
        }
        
        // function for movement
        var movement = this.setNewOffset.bind(this);
        
        // function for release
        var release = function release() {
            $(document)
                .off('mousemove', movement)
                .off('mouseup', release);
        }
        
        $(document)
            .on('mousemove', movement )
            .on('mouseup', release );
    },
    
    grabLeftHandle: function(e) {

        this.grabHandle(e);
        
    },
    
    grabRightHandle: function(e) {

        this.grabHandle(e, true);
        
    },
    
    grabHandle: function(e, right) {
        
        e.stopPropagation();
        e.originalEvent.preventDefault();
        
        // Get beginning state of recording
        var startX = e.clientX;
        var startState = this.model.serialize();
        var curWidth = startState.viewport_width;
        var curLeft = startState.viewport_left;
        
        function rMvmt(ev) {
            // calculate movement delta
            var delta = ev.clientX - startX;
            // console.log('delta: ', delta);
            
            if (delta < 0) {
                // if the width is equal to or less than
                // the viewport_min_limit, 
                if (curWidth <= startState.viewport_min_limit) {
                    // move the viewport (but not past 0).
                    var newLeft = Math.max(startState.viewport_left + delta, 0);
                    this.$vp.css({
                        'left': newLeft
                    });
                    curLeft = newLeft;
                }
                // otherwise reduce the width by delta
                else {
                    // WORKS
                    var newWidth = startState.viewport_width + delta
                    this.$vp.css({
                        'width': newWidth+'px'
                    });
                    curWidth = newWidth;
                }
                
            }
            else if (delta > 0) {
                // if the width + offset is less than scroller_width,
                // increase the width by delta
                if ( (curWidth + curLeft) <= startState.scroller_width ) {
                    // WORKS
                    var max_width = startState.scroller_width - curLeft;
                    var newWidth = Math.min(max_width,startState.viewport_width + delta);
                    this.$vp.css({
                        'width': newWidth+'px'
                    });
                    curWidth = newWidth;
                    
                }
            }
        }
        
        function lMvmt(ev) {
            // calculate movement delta
            var delta = (ev.clientX - startX); // negate to make more sense

            if (delta < 0) {
                // if the offset is greater than 0 change width and offset by same amout
                if (curLeft > 0) {
                    var max_change = Math.min(curLeft, delta);
                    var newLeft = startState.viewport_left + max_change;
                    var newWidth = startState.viewport_width - max_change;
                    this.$vp.css({
                        'width': newWidth+'px',
                        'left': newLeft+'px'
                    });
                    
                    curWidth = newWidth;
                    curLeft = newLeft;
                }
            }
            else if (delta > 0) {
                // if the width is equal to or less than
                // the viewport_min_limit, move the viewport 
                // (but not past scroller_width - viewport_min_limit).
                if (curWidth <= startState.viewport_min_limit) {
                    var newLeft = startState.viewport_left - delta;
                    newLeft = Math.min(newLeft, startState.scroller_width - startState.viewport_min_limit);
                    this.$vp.css({
                        'left': newLeft
                    });
                    curLeft = newLeft;
                }
                // otherwise reduce width by delta and change offset by same amount
                else {
                    var newLeft = Math.min(0, startState.viewport_left - delta);
                    newLeft = Math.min(newLeft, startState.scroller_width - startState.viewport_min_limit);
                    var newWidth = startState.viewport_width + delta;
                    this.$vp.css({
                        'width': newWidth+'px'
                    });
                    curWidth = newWidth;
                }
            }
        }
        
        var movement = right ? rMvmt.bind(this) : lMvmt.bind(this) ;
            
        var release = function release(ev) {
            // when changing limit, make silent so as 
            // not to trigger offset changes
            $(document)
                .off('mousemove', movement)
                .off('mouseup', release);
        }
        
        $(document)
            .on('mousemove', movement)
            .on('mouseup', release);
        
    },
    
    template: kt.make(__dirname+'/Overview.html','_')
    
});
exports = module.exports = Overview;