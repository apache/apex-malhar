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
 * Control interface for the performance metrics chart
 * 
*/
var _ = require('underscore');
var kt = require('knights-templar');
var Notifier = DT.lib.Notifier;
var BaseView = require('bassview');
var bbindings = DT.lib.Bbindings;
var Ctrl = BaseView.extend({
    
    initialize: function(options) {
        this.state = options.state;
        this.widgetModel = options.widgetModel;

        var defaultOptions = {
            multi_y: true,
            render_points: true
        };

        var options = this.widgetModel.pick('multi_y', 'render_points');
        _.defaults(options, defaultOptions);

        this.model.set(options, { silent: true });
        
        // Bind controls to the 
        // appropriate model attributes
        // this.subview('toggle_multi_y', new bbindings.checkbox({
        //     attr: 'multi_y',
        //     model: this.model
        // }));
        
        this.subview('metric_limit', new bbindings.select({
            attr: 'limit',
            model: this.state
        }));
        
        // this.subview('toggle_render_points', new bbindings.checkbox({
        //     attr: 'render_points',
        //     model: this.model
        // }));
        
        this.setupPlotViews();
        this.listenToDisplayOptions();
    },
    
    setupPlotViews: function() {
        this.trigger('removePlotViews');
        this.model.plots.each(function(plot) {
            var plotview = new bbindings.checkbox({
                attr: 'visible',
                model: plot
            });

            plotview.listenTo(this, 'removePlotViews', plotview.remove);
            this.subview(plot.get('key'), plotview);
        }, this);
        this.render();
    },

    listenToDisplayOptions: function() {
        // this.listenTo(this.model, 'change:multi_y', function(model) {
        //     this.widgetModel.set('multi_y', model.get('multi_y'));
        // });
        // 
        // this.listenTo(this.model, 'change:render_points', function(model) {
        //     this.widgetModel.set('render_points', model.get('render_points'));
        // });

        this.listenTo(this.state, 'change:limit', function(model) {
            this.widgetModel.set('limit', model.get('limit'));
        });

        this.model.plots.each(function(plot) {
            this.listenTo(plot, 'change:visible', this.plotChanged);
        }, this);
    },

    plotChanged: function(plot) {
        this.widgetModel.set(plot.get('key'), plot.get('visible'));
    },

    setPlotAssignments: function(assignments) {
        this.model.plots.each(function(plot){
            var key = plot.get('key');
            assignments['input[data-key="'+key+'"]'] = key;
        }, this);
    },
    
    render: function() {
        
        var assignments = {
            // '.toggle_multi_y' : 'toggle_multi_y',
            '.metric_limit' : 'metric_limit',
            // '.toggle_render_points' : 'toggle_render_points'
        }
        var json = this.model.serialize();
        var markup = this.template(json);
        this.$el.html(markup);
        this.setPlotAssignments(assignments);
        this.assign(assignments);

        return this;
    },
    
    template: kt.make(__dirname+'/Ctrl.html','_')
    
});

exports = module.exports = Ctrl;