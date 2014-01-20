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
var RecordingCollection = DT.lib.RecordingCollection;
var RecordingModel = require('./lib/OpChartModel');
var RecView = require('./lib/OpChartView');
var Control = require('./lib/Control');
var bormat = require('bormat');
var BaseView = DT.widgets.Widget;

/**
 * OpChart Widget
 * 
 * Charting widget for chart operators
*/
var OpChart = BaseView.extend({

    initialize:function(options) {
        
        BaseView.prototype.initialize.call(this,options);

        // injections
        this.appId = this.model.get('id');
        this.dataSource = options.dataSource;
        
        // create composite key for this
        // widget and instance
        this.wKey = this.compId(this.appId);
        
        // set up recording model, view, and collection
        this.recording = new RecordingModel({
            appId: this.appId,
            scroller_height: 30
        }, { dataSource: this.dataSource });
        this.recordings = new RecordingCollection([], {
            dataSource: this.dataSource,
            appId: this.appId
        });
        
        // Listen for updates of recordings to re-rendering
        this.listenTo(this.recordings, 'sync', this.render);
        
        // Listen for updates to this.recording to save
        this.listenTo(this.recording, 'change:limit change:tail', this.saveRecordingState);

        // Check for previously stored attributes for this chart
        var previous = localStorage.getItem('ops.apps.opchart.'+this.wKey);
        if (previous) {
            try {
                var json = JSON.parse(previous);
                if (typeof json !== "object") throw '';
                this.setRecording(json);
            } catch (e) {
                localStorage.removeItem('ops.apps.opchart.'+this.wKey);
            }
        }
        
        if (!this.recording.get('startTime')) {
            // No previous recording set or found, fetch recordings
            this.recordings.fetch();
        }
        
    },
    
    html: function() {
        var json, markup;
        
        // Check if recording has been set.
        if (this.recording.get('startTime')) {
            // It has. Render mark-up from the chart_template.
            // Assumes the presence of the 'chart' subview.
            json = this.recording.toJSON();
            markup = this.chart_template(json);
        }
        else {
            json = { options: this.getChartOptions(this.recordings) }
            markup = this.blank_template(json);
        }
        
        return markup;
    },

    assignments: function() {
        if (this.recording.get('startTime')) {
            return {
                '.chart-target': 'chart',
                '.ctrl-target': 'control'
            };
        } else {
            return {};
        }
    },
    
    getChartOptions: function(recs) {
        var result = _.filter(recs.toJSON(), function(rec) {
            rec['startTimeFormatted'] = bormat.timeSince(rec['startTime']);
            return rec.properties.hasOwnProperty('chartType');
        }, this);
        return result;
    },
    
    selectRecording: function(evt) {
        evt.preventDefault();
        var recName = this.$('select.opchart-options').val();
        var recording = this.recordings.where({ startTime: recName })[0];
        this.setRecording(recording.toJSON());
    },
    
    setRecording: function(recordingJSON) {

        // Remove previous views
        _.each(['chart','control'], function(key) {
            var sv = this.subview(key);
            if (sv) {
                sv.remove();
            }
        }, this);
        
        // Set the recording property
        this.recording.set( recordingJSON );
        
        // Store chart type
        var chartType = recordingJSON.properties.chartType.toLowerCase();
        
        // Check for compatibility
        if (! RecView.hasOwnProperty(chartType) || ! Control.hasOwnProperty(chartType) ) {
            throw new Error('This chart type ('+chartType+') is not supported.');
        }
        
        // Set up chart view
        var chart = new RecView[chartType]({
            model: this.recording,
            collection: this.recording.tuples,
            dataSource: this.dataSource,
            id: this.wKey
        });
        
        // Set up controls view
        var control = new Control[chartType]({
            model: chart.viewport.model,
            recording: this.recording
        });
        
        this.subview('chart', chart);
        this.subview('control', control);
        
        // Set the chart model on the recording
        this.recording.setChart(chart.viewport);
        
        // Render this widget
        this.renderContent();
        
    },
    
    saveRecordingState: function() {
        var json = this.recording.toJSON();
        localStorage.setItem('ops.apps.opchart.'+this.wKey, JSON.stringify(json) );
    },
    
    remove: function(){
        this.recording.unsubscribe();
        this.recording.tuples.reset([]);
        BaseView.prototype.remove.call(this);
    },
    
    events: {
        'click .chartRecording': 'selectRecording'
    },
    
    blank_template: kt.make(__dirname+'/opchart_setup.html','_'),
    
    chart_template: kt.make(__dirname+'/OpChartWidget.html','_')
    
});
exports = module.exports = OpChart;
