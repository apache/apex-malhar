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
 * OpChartView
 * 
 * View for an operator chart (application data).
*/
var _ = require('underscore');
var BigInteger = require('jsbn');
var util = require('./util');
var kt = require('knights-templar');
var BaseView = require('bassview');
var Chart = DT.lib.LiveChart;
var Overview = require('./Overview');
var LineOpChartView = BaseView.extend({
    
    initialize: function(options) {
        // this.model      : OpChartModel
        // this.collection : OpChartModel.tuples
        
        this.dataSource = options.dataSource;
        this.viewport = new Chart(
            this.model.createViewportOptions(options.id)
        );
        
        // subviews:
        // viewport
        this.subview('viewport', this.viewport.view);
        
        // overview
        this.subview('overview', new Overview({
            model: this.model,
            vpChart: this.viewport,
            id: options.id+'.overview'
        }));
        
        // Listen
        this.listenTo(this.model, 'change:offset change:limit', this.renderChartData);
        this.listenTo(this.collection, 'update', this.renderChartData);
        
    },
    
    renderChartData: function() {
        // Get recording
        var recording = this.model;
        
        // Get its offset
        var offset = this.model.get('offset');
        
        // Get a reference to the viewport collection
        var vpData = this.viewport.data;
        
        // Get the tuples at the current offset
        var tuples = [];
        var i = new BigInteger(offset+'');
        var limit = i.add(new BigInteger(recording.get('limit')+''));
        for (i; i.compareTo(limit); i = i.add(BigInteger.ONE)) {
            
            var tuple = this.collection.get(i.toString());
            if (tuple) {
                // add the data to the tuples list
                tuples = tuples.concat(tuple.get('data'));
            }
            
        }
        
        // Check if empty and there are no plots
        if (tuples.length === 0) {
            // no data, do nothing
            return;
        }
        
        // Check that plots have been auto-detected
        if (this.viewport.model.plots.length === 0) {
            var plots = util.extractPlots(recording.get('properties'), tuples[0]);
            this.viewport.plot(plots);
        }
        
        // Transform the tuple data into format for charted
        tuples = util.transformTupleData(tuples, this.viewport.model.get('mode') );
        
        // Reset the viewport data with the new stuff
        vpData.reset(tuples);
        
    },
    
    render: function() {
        var json = this.model.toJSON();
        var markup = this.template(json);
        this.$el.html(markup);
        this.assign({
            '.opchart-viewport':'viewport',
            '.opchart-overview':'overview'
        });
        return this;
    },
    
    template: kt.make(__dirname+'/LineOpChartView.html','_')
    
});
exports = module.exports = LineOpChartView;