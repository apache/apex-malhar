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
var kt = require('knights-templar');
var BaseView = require('./base');
var d3 = require('d3');
var Pie = BaseView.extend({
    
    render: function() {
        this.$el.html(this.template({}));
        var width = 300,
            height = 300,
            radius = Math.min(width, height) / 2,
            valAttr = this.valAttribute;
        
        this.legend = this.createLegend();
        
        this.arc = d3.svg.arc()
            .outerRadius(radius - 10)
            .innerRadius(0);

        this.pie = d3.layout.pie()
            .sort(function(a,b) {
                return b[valAttr] - a[valAttr]
            })
            .value(function(d) { return d[valAttr]; });

        this.svg = d3.select(this.$('.pie-chart')[0]).append("svg")
            .attr("width", width)
            .attr("height", height)
            .append("g")
            .attr("transform", "translate(" + width / 2 + "," + height / 2 + ")");
        
        return this;
    },
    
    onData: function(data) {
        
        BaseView.prototype.onData.call(this, data);
        
        this.updateLegend(data);
        
        this.updateChart(data);
        
    },
    
    updateChart: function(data) {
        var colors = this.colors;
        var arc = this.arc;
        var pie = this.pie;
        var svg = this.svg;
        var idAttr = this.idAttribute;
        var transformTextFn = function(d) { return "translate(" + arc.centroid(d) + ")" };

        var g = svg.selectAll(".arc")
            .data(pie(data), function(d) {
                return d.data[idAttr];
            });
        
        var ngroups = g.enter().append("g")
            .attr("class", "arc")
            .style("opacity", 0);
            
        g.exit().transition().duration(500).style("opacity", 0).remove();

        ngroups.append("path")
            .attr("d", arc)
            .each(function(d) {
                this._current = d;
            })
            .style("fill", function(d) {
                return colors[d.data[idAttr]];
            });
        
        // ngroups.append('text')
        // .attr('transform', transformTextFn)
        //     .attr('dy', '.35em')
        //     .style('text-anchor', 'middle')
        //     .text(function(d) { return d.data[idAttr] });
         
        g.select("path").transition().duration(500).attrTween("d", function(a) {
            var i = d3.interpolate(this._current, a);
            this._current = i(0);
            return function(t) {
                return arc(i(t));
            }
        });
        // g.select("text").transition().duration(500).attr('transform', transformTextFn);
        

        ngroups.transition()
        .duration(500)
        .style("opacity", 1)
    },
    
    template: kt.make(__dirname+'/pie.html','_')
    
});
exports = module.exports = Pie;