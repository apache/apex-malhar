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
var BaseView = require('../PerfMetricsWidget');
var Ctrl = require('./Ctrl');
var Appmetrics = BaseView.extend({

    defaultSeriesOptions: {
        tuplesEmittedPSMA10: true,
        tuplesProcessedPSMA10: true,
        cpuPercentageMA10: true,
        latencyMA: true,
        inputBufferServerBytesPS: false,
        outputBufferServerBytesPS: false
    },

    initialize: function(options) {
        // Call super init
        BaseView.prototype.initialize.call(this, options);
        
        // Pick up injections
        this.dataSource = options.dataSource;

        // Init the chart
        this.chart = new Chart({
            width: 700,
            height: 400,
            id: 'ops.appmetrics'+this.compId(),
            time_key: 'lastHeartbeat',
            time_range: this.state.get('limit') * 1000,
            min_y_range: 10,
            type: 'line'
        });
        
        // Plot the initial series
        var series =[{
            key: "tuplesEmittedPSMA",
            color: "#64c539",
            label: DT.text('emitted_per_sec')
        }, {
            key: 'tuplesProcessedPSMA',
            color: '#1da8db',
            label: DT.text('processed_per_sec')
        },{
            key: 'cpuPercentageMA',
            color: '#da1c17',
            label: DT.text('cpu_percentage_label')
        }, {
            key: 'latencyMA',
            color: '#888',
            label: DT.text('latency_ms_label')
        }, {
            key: 'inputBufferServerBytesPS',
            color: '#AE08CE',
            label: DT.text('buffer_server_reads_label')
        }, {
           key: 'outputBufferServerBytesPS',
           color: '#f2be20',
           label: DT.text('buffer_server_writes_label')
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
        var point = this.model.serialize(true, true);
        data.add(point);
        if (data.length > this.state.get('limit')) {
            var over = data.length - this.state.get('limit')*1;
            var models = data.models.slice();
            models.splice(0,over);
            data.reset(models);
        }
    }
    
});

exports = module.exports = Appmetrics