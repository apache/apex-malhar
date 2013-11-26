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
var _ = require('underscore');
var d3 = require('d3');
var kt = require('knights-templar');
var BaseView = require('./base');
var HorizontalBar = BaseView.extend({
    
    render: function() {
        var parent;
        this.$el.html(this.template({}));
        parent = this.$('.bar-chart')[0];
        this.chart = d3.select(parent).append('div').attr('class', 'hbar-inner');
        this.scale = d3.scale.linear();
        this.axis = d3.svg.axis().orient('top').scale(this.scale);
        this.updateScaleRange();
        this.barHeight = 20;
        return this;
    },
    
    onData: function(data) {
        BaseView.prototype.onData.call(this, data);
        var idAttr = this.idAttribute;
        var valAttr = this.valAttribute;
        var data = data.slice().sort(function(a,b) { return b[valAttr] - a[valAttr] });
        var datalength = data.length;
        var colors = this.colors;
        var scale;
        var barHeight = this.barHeight;
        var containerHeight = (datalength + 1) * (barHeight + 2);
        var bars = this.chart.style('height', containerHeight + 'px').selectAll('.bar')
            .data(data, function(d) { return d[idAttr] });
        
        // update the scale
        this.updateScaleRange();
        scale = this.updateScaleDomain(data);
        
        bars.enter().append('div')
            .attr('class', 'bar')
            .style('opacity', 1)
            .style('background-color', function(d) {
                return colors[d[idAttr]];
            });
            
        bars
            .html(function(d) {
                return '<span class="bar-label">' + d[idAttr] + ' (' + d[valAttr] + ')</span>';
            })
            .transition().duration(500)
            .style({
                'height': (barHeight - 2) + 'px',
                'line-height': (barHeight - 2) + 'px',
                'top': function(d,i) {
                    return (i * barHeight) + 'px';
                },
                'width': function(d) {
                    return scale(d[valAttr]);
                }
            })
            
        
        bars.exit().transition().duration(500).style({
            'height': '0px',
            'width': '0px',
            'opacity': 0
        }).remove();
        
    },
    
    onResize: function() {
        this.updateScaleRange();
        this.onData(this.widgetView.data);
    },
    
    updateScaleDomain: function(data) {
        var valAttr = this.valAttribute;
        var newDomain = [
            0, 
            d3.max(data, function(d) {
                return d[valAttr];
            })
        ];
        return this.scale.domain(newDomain);
        
    },
    
    updateScaleRange: function() {
        var width = this.$el.width();
        return this.scale.range(['0px', width + 'px']);
    },
    
    template: kt.make(__dirname+'/bar.html','_')
    
});
exports = module.exports = HorizontalBar;