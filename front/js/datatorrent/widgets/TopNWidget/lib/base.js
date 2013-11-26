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
var mrcolor = require('mrcolor');
var BaseView = require('bassview');
var BaseVisual = BaseView.extend({
    
    initialize: function(options) {
        this.on('data', this.onData);
        this.colors = {};
        this.mrcolor = mrcolor();
        
        // i dont like boring colors
        for (var i = 0; i < 6; i++) {
            this.mrcolor();
        }
        
        // set id and val attributes
        this.idAttribute = options.idAttribute;
        this.valAttribute = options.valAttribute;
        this.widgetView = options.widgetView;
    },
    
    onResize: function() {
        // can be implemented by subclasses
    },
    
    onData: function(data) {
        
        var valAttr = this.valAttribute;
        
        // assign colors
        _.each(data, function(val, key) {
            var color;
            if ( !this.colors.hasOwnProperty(val[this.idAttribute]) ) {
                color = this.colors[val[this.idAttribute]] = 'rgb(' + this.mrcolor().rgb().join(',') + ')';
            } else {
                color = this.colors[val[this.idAttribute]]
            }
            val['_topn_color_'] = color;
        }, this);
        
        // ensure numeric for value
        data.forEach(function(d) {
            d[valAttr] = +d[valAttr];
        });
    },
    
    createLegend: function() {
        return d3.select(this.$('.topn-legend')[0]).append('ol');
    },
    
    updateLegend: function(data) {
        var data = data.slice(), // copy data set
            colors = this.colors,
            idAttr = this.idAttribute,
            valAttr = this.valAttribute,
            legend = this.legend,
            items;
        
        data.sort(this.sortFn)
        
        items = legend.selectAll('.legend-item')
            .data(data, function(d) {
                return d[idAttr];
            });
        
        items.enter().append('li')
            .attr('class', 'legend-item');
        
        items.exit().remove();
        
        items.sort(function(a,b) {
            return b[valAttr] - a[valAttr];
        })
        .html(function(d) {
            return '<span class="color-box" style="background-color:' + colors[d[idAttr]] + '"></span> ' + d[idAttr] + ' (' + d[valAttr] + ')';
        });
    }
    
});
exports = module.exports = BaseVisual;