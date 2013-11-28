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
 * TopNWidget
 * 
 * Displays position-comparable data as a pie chart, bar chart, etc.
 *
*/

var _ = require('underscore');
var Backbone = require('backbone');
var kt = require('knights-templar');
var BaseView = require('../../WidgetView');
var bbind = require('../../Bbindings');

// visualization modes
var modes = require('./modes');

// class definition
var TopNWidget = BaseView.extend({
    
    initialize: function(options) {
        
        BaseView.prototype.initialize.call(this, options);
        
        // get the datasource
        this.dataSource = options.dataSource;
        
        // Set up widget definition "renderMode" attribute if not already set
        if (! this.widgetDef.get('renderMode') ) {
            this.widgetDef.set('renderMode', 'pie');
        }
        
        // Create data
        this.data = [];
        
        // listeners, subviews, etc.
        this.subview('renderMode', new bbind.select({
            model: this.widgetDef,
            attr: 'renderMode'
        }));
        
        // Creates the visualization subview
        this.createVisual();
        
        // Listen for rendermode changes
        this.listenTo(this.widgetDef, 'change:renderMode', this.createAndRenderVisual);
        
        // Subscribe to the specific topic
        this.subscribeToTopic();
        
        /// DEV ///
        function generateRandom() {
            
            var res = [
                { "name" : "www.example.com/entity1", "value": Math.floor( 0 + Math.random() * 15) },
                { "name" : "www.example.com/entity2", "value": Math.floor(10 + Math.random() * 15) },
                { "name" : "www.example.com/entity3", "value": Math.floor(20 + Math.random() * 15) },
                { "name" : "www.example.com/entity4", "value": Math.floor(30 + Math.random() * 15) },
                { "name" : "www.example.com/entity5", "value": Math.floor(40 + Math.random() * 15) },
                { "name" : "www.example.com/entity7", "value": Math.floor(50 + Math.random() * 15) },
                { "name" : "www.example.com/entity6", "value": Math.floor(60 + Math.random() * 15) }
            ];
            
            var i = Math.floor(Math.random() * res.length);
            
            if (i % 3 === 0) res.splice(i, 1);
            
            return res;
        }
        this._interval_key_ = setInterval(_.bind(function() {
            this.dataSource.trigger(this.subscribeTopic, generateRandom());
        }, this), 2000);
        /// DEV ///
    },
    
    onResize: function() {
        var visual;
        
        if (visual = this.subview('visual')) {
            visual.onResize();
        }
    },
    
    idAttribute: 'name',
    
    valAttribute: 'value',
    
    subscribeTopic: 'example.topic',
    
    contentClass: 'topn-content',
    
    subscribeToTopic: function() {
        var topic = _.result(this, "subscribeTopic");
        this.listenTo(this.dataSource, topic, function(data) {

            // Holds current view
            var curVisual;
            
            // Set the data
            this.data = data;
            
            // Notify current visualization
            if (curVisual = this.subview('visual')) {
                curVisual.trigger('data', this.data);
            }
            
        });
        this.dataSource.subscribe(topic);
    },
    
    createVisual: function() {
        var mode = this.widgetDef.get('renderMode'), oldView;
        
        // destroy previous view
        if (oldView = this.subview('visual')) {
            oldView.remove();
        }
        
        // create new view
        this.subview('visual', new modes[mode]({
            idAttribute: this.idAttribute,
            valAttribute: this.valAttribute,
            widgetView: this
        }));
        
    },
    
    createAndRenderVisual: function() {
        this.createVisual();
        this.renderContent();
        
        // update with current data after render
        if (this.data.length) {
            this.subview('visual').trigger('data', this.data);
        }
    },
    
    assignments: function() {
        var curVisual, assignments = {
            '.renderMode': 'renderMode'
        };
        
        if (curVisual = this.subview('visual')) {
            assignments['.topn-visual'] = 'visual';
        }
        
        return assignments;
    },
    
    template: kt.make(__dirname+'/TopNWidget.html','_'),
    
    /// DEV ///
    remove: function() {
        clearInterval(this._interval_key_);
        BaseView.prototype.remove.call(this);
    }
    /// DEV ///
});

exports = module.exports = TopNWidget;