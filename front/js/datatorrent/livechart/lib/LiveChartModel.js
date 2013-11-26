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
var Backbone = require('backbone');
var Datum = Backbone.Model.extend({
    idAttribute: '_id'
});
var Data = Backbone.Collection.extend({
    model: Datum
});
var Plot = Backbone.Model.extend({
    defaults: {
        'label': '',
        'color': '#777',
        'visible': true
    },
    idAttribute: 'key'
});
var Plots = Backbone.Collection.extend({
    model: Plot
});
var LiveChartModel = Backbone.Model.extend({
    
    defaults: {
        // current height of the chart
        width: 800,
        // current width of the chart
        height: 500,
        // margins for the chart
        margin: {
            top: 0,
            right: 0,
            bottom: 40,
            left: 60
        },
        // interpolation algo for line graph
        interpolation: 'cubic-in-out',
        // interpolation for animation
        easing: 'cubic-in-out',
        // duration for animation (should be less than minimum time til next point)
        duration: 500,
        // amount of time to display on the x axis
        time_range: 10000,
        // the key of the data points to use as x value
        time_key: 'time',
        // show y guides
        y_guides: true,
        // when true, assumes that all points come in chronologically (makes )
        // assume_chron: true
        // Initial set of plots/series
        plots: [],
        // y-axis label format string for d3.format().
        // @see https://github.com/mbostock/d3/wiki/Formatting#wiki-d3_format
        y_label_format: 's',
        // pads the y-domain for a nicer looking plot
        domain_padding: 0.05,
        // This is the minimum range that the y axis can be
        min_y_range: 0,
        // If set to false, will not show y values on mouseover
        mouse_over: true
    },
    
    initialize: function(attrs,options) {
        this._setUpData(attrs);
        this._setUpPlots(attrs.plots);
        this.loadStoredInfo();
    },
    
    serialize: function() {
        var json = this.toJSON();
        json['plots'] = this.plots.toJSON();
        json['data'] = json['data'].toJSON();
        return json;
    },
    
    // Override if necessary in derived classes
    _setUpData: function(attrs) {
        this.set('data', new Data(attrs.data));
    },
    
    // Override if necessary in derived classes
    _setUpPlots: function(plots) {
        this.plots = new Plots(plots || []);
    },
    
    // Add a plot/series to the chart.
    plot: function(plots) {
        
        var plots = [].concat(plots);
        
        for (var i = 0; i < plots.length; i++){
            var plot = plots[i];
            plot.label = plot.label || plot.key;
            if (plot.key === undefined) {
                throw new TypeError('Plot definition objects must contain a "key" field');
            }
        };

        this.plots.add( plots, { merge: true } );
        
    },
    
    // Removes plot from chart.
    unplot: function(key) {
        var model = this.plots.get(key);
        if (model) {
            this.plots.remove(model);
        }
    },
    
    // Extracts series data from the main data array
    generateSeriesData:function() {

        var time_key = this.get('time_key'),
            data = this.get('data');
        
        var visible_plots = this.plots.where({'visible': true});
    
        return _.map(visible_plots, function(plot) {
            var key = plot.get('key');
            return {
                plot: plot.toJSON(),
                key: key,
                values: data.map(function(point) {
                    var val = +point.get(key);
                    return { time: point.get(time_key), value: isNaN(val) ? 0 : val };
                })
            }
            
            
        });
    },
    
    // This function looks up any and all values that should be restored from previous sessions.
    loadStoredInfo: function() {
        
        // Check for specified id
        if (!this.get("id")) return;
        
        // Check for state
        var prevState = this.state();
        
        // Restore it
        this.set(prevState, { validate: true });
        
    },
    
    // Set or get an attribute in localStorage
    state: function(key, value) {
        
        var storage_key = 'livechart.' + this.get("id");
        var store = this.store || localStorage.getItem(storage_key) || {};
        
        if (typeof store !== "object") {
            try {
                store = JSON.parse(store);
            } catch(e) {
                store = {};
            }
        }
        this.store = store;
        
        if (value !== undefined) {
            store[key] = value;
            localStorage.setItem(storage_key, JSON.stringify(store));
            return this;
        } else if (key !== undefined) {
            return store[key];
        } else {
            return store;
        }
        
    },
});

exports = module.exports = LiveChartModel;