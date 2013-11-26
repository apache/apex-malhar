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
 * Scroller View
 * 
 * This is the view for the narrow scrolling element to the 
 * right of the console. Renders color wherever tuples have already
 * been loaded.
*/
var _ = require('underscore');
var kt = require('knights-templar');
var BaseView = require('bassview');
var bigRat = require('big-rational');
var ScrollerView = BaseView.extend({
    
    initialize: function(options) {
        this.listenTo(this.model.tuples, 'add change:portId', this.renderCanvasPreview);
        this.listenTo(this.model.tuples, 'reset', this.render);
        this.listenTo(this.model, 'change:offset', this.setViewport);
        
    },
    
    render: function() {
        // Set up mark-up
        var json = this.model.serialize(true);
        var markup = this.template(json);
        this.$el.html(markup);
        
        // Store instance vars
        this.$viewport = this.$(".viewport");
        this.$ixn = this.$('.interaction');
        this.canvas = this.$("canvas")[0];
        this.ctx = this.canvas.getContext('2d');
        
        return this;
    },
    
    canvasColors: {
        'loading': '#DEDEDE',
        'input':   '#7ec2db',
        'output':  '#7ccd76',
        'error':   '#c06955'
    },
    
    renderCanvasPreview: function(model) {
        var tv_json = this.model.serialize();
        var port = this.model.ports.where({ id: model.get('portId') })[0];
        if (port === undefined) return;
        var type = port.get("type");
        var strokeColor = this.canvasColors[type];
        var offset = model.get("idx");
        var total = tv_json.currentTotal;
        if (total == 0) return;
        var lineWidth = Math.max( 1, bigRat(1, total).times(tv_json.scrollerHeight).valueOf() );
        var canvasOffset = bigRat(offset).over(total).multiply(bigRat(tv_json.scrollerHeight));
        this.ctx.strokeStyle = strokeColor;
        this.ctx.lineWidth = lineWidth.valueOf();
        this.ctx.beginPath();
        this.ctx.moveTo(0,canvasOffset.valueOf());
        this.ctx.lineTo(tv_json.scrollerWidth, canvasOffset.valueOf());
        this.ctx.stroke();
    },
    
    setViewport: function() {
        var tv_json = this.model.serialize(true);
        this.$viewport.css('top',tv_json.scrollViewportTop+'px');
        return this;
    },
    
    events: {
        
        "mousedown .interaction": "onMouseDown",
        "mouseout .interaction": "onMouseOut"
        
    },
    
    onMouseDown: function(e) {
        this.setNewOffset(e);
        this.grabViewport(e);
    },
    
    onMouseOut: function(e) {
        this.letGoViewport(e);
    },
    
    setNewOffset: function(e) {
        // BigInteger
        // set new offset
        var tv_json = this.model.serialize();
        var vpHeight = tv_json.scrollViewportHeight;
        var mouseOffset = e.clientY - this.$el.offset().top + window.pageYOffset;
        mouseOffset = Math.min( tv_json.scrollerHeight - vpHeight, mouseOffset);
        
        var recOffset = bigRat(mouseOffset/tv_json.scrollerHeight).times(tv_json.currentTotal).round().toDecimal();
        this.model.set({"offset": recOffset},{validate: true});
    },
    
    letGoViewport: function(e) {
        this.$el.off("mousemove");
        this.$el.off("mouseup");
    },
    
    grabViewport: function(e) {
        if (e) {
            e.stopPropagation();
            e.originalEvent.preventDefault();
        }
        this.$el.on("mousemove", this.setNewOffset.bind(this) );
        this.$el.on("mouseup", this.letGoViewport.bind(this) );
    },
    
    template: kt.make(__dirname+'/ScrollerView.html','_')
    
});
exports = module.exports = ScrollerView;