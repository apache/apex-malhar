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
 * App metric data chart
*/
var _ = require('underscore');
var kt = require('knights-templar');
var Chart = DT.lib.LiveChart;
var BaseView = DT.widgets.Widget;
var MetricState = require('./MetricState');
var Ctrl = require('./Ctrl');
var PerfMetricsWidget = BaseView.extend({
    
    initialize: function(options) {
        // Call super init
        BaseView.prototype.initialize.call(this, options);

        if (this.defaultSeriesOptions) {
            _.defaults(this.widgetDef.attributes, this.defaultSeriesOptions);
        }

        var defaultStateOptions = {
            limit: 50
        };
        var stateOptions = this.widgetDef.pick('limit');
        _.defaults(stateOptions, defaultStateOptions);

        // Create a state model for the metric graph
        this.state = new MetricState(stateOptions);
        
        // Listen for changes to limit to update the chart
        this.listenTo(this.state, 'change:limit', function(model, new_range) {
            if (this.chart) {
                this.chart.model.set('time_range', new_range * 1000);
            }
        });
    },
    
    onResize: function() {
        this.chart.view.resizeChartToCtnr();
    },

    plotSeries: function(series) {
        _.each(series, function(plot) {
            plot.visible = this.widgetDef.get(plot.key);
            this.chart.plot(plot);
        }, this);
    },
    
    addPoint: function(point) {
        var data = this.chart.data;
        var point = point || this.model.serialize(true);
        var time_key = this.chart.model.get('time_key');
        var now = +new Date();
        var limit = this.state.get('limit') * 1000;
        var delay = this.state.get('delay') * 1000;
        data.add(point);
        while(data.at(0) && data.at(0).get(time_key) < (now - limit - delay)) {
            data.shift();
        }
    },
    
    render: function() {
        BaseView.prototype.render.call(this);
        if (!this.chart.model.state('vp_width')) {
            setTimeout(function() {
                this.chart.view.resizeChartToCtnr();
            }.bind(this), 300);
        }
        return this;
    },
    
    assignments: {
        '.perfmetrics-chart-ctnr': 'chart',
        '.perfmetrics-chart-ctrl': 'ctrl'
    },

    contentClass: 'perfmetrics-content',

    template: kt.make(__dirname+'/PerfMetricsWidget.html','_')
    
});

exports = module.exports = PerfMetricsWidget;