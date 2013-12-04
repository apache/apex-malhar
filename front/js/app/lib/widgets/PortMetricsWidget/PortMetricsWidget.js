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
 * Port metric data chart
*/
var _ = require('underscore');
var kt = require('knights-templar');
var Chart = DT.lib.LiveChart;
var BaseView = require('../PerfMetricsWidget');
var Ctrl = require('./Ctrl');
var PortMetricsWidget = BaseView.extend({

    defaultSeriesOptions: {
        tuplesPSMA10: true,
        bufferServerBytesPSMA10: true
    },

    initialize: function(options) {
        // Call super init
        BaseView.prototype.initialize.call(this, options);
        
        // Pick up injections
        this.dataSource = options.dataSource;
        
        // Set buffer server label based on type
        var bufferServerBytesLabel = this.model.get('type') === 'input' ? 'Reads' : 'Writes';
        
        // Init the chart
        this.chart = new Chart({
            width: 700,
            height: 400,
            id: 'ops.portmetrics'+this.compId(),
            time_key: 'timestamp',
            time_range: this.state.get('limit') * 1000,
            min_y_range: 10,
            type: 'line'
        });
        
        // Plot the initial series
        var series = [{
            key: "tuplesPSMA",
            color: "#64c539",
            label: DT.text('tuples_per_sec')
        }, {
            key: 'bufferServerBytesPSMA',
            color: '#f2be20',
            label: 'Buffer Server ' + bufferServerBytesLabel + '/sec'
        }];

        this.plotSeries(series);

        // Register as a subview
        this.subview('chart', this.chart.view);
        
        // Chart control
        this.subview('ctrl', new Ctrl({
            model: this.chart.model,
            state: this.state,
            widgetModel: this.widgetDef
        }));
        
        // Listen to instance for changes
        this.listenTo(this.model, 'update', this.addPoint);
    },
    
    addPoint: function() {
        var data = this.chart.data;
        var point = this.model.toJSON();
        point.timestamp = +new Date();
        data.add(point);
        if (data.length > this.state.get('limit')) {
            var over = data.length - this.state.get('limit')*1;
            var models = data.models.slice();
            models.splice(0,over);
            data.reset(models);
        }
    }
    
});

exports = module.exports = PortMetricsWidget