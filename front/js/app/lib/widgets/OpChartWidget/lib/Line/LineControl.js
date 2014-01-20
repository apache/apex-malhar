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
 * OpChart Control View
 * 
 * This is the controls view for opchart widget.
*/
var Notifier = DT.lib.Notifier;
var BaseView = require('bassview');
var _ = require('underscore');
var kt = require('knights-templar');
var bbindings = DT.lib.Bbindings;
var OffsetCtrl = require('./OffsetCtrl');
// var OffsetCtrl = require('./')
var Control = BaseView.extend({
    
    initialize: function(options) {
        
        this.recording = options.recording;
        
        this.subview('toggle_tail', new bbindings.checkbox({
            attr: 'tail',
            model: this.recording
        }));
        
        this.subview('rec_offset', new OffsetCtrl({
            attr: 'offset',
            model: this.recording
        }));
        
        this.subview('rec_limit', new bbindings.textint({
            attr: 'limit',
            model: this.recording
        }));
        
        this.listenTo(this.model.plots, 'add remove', this.setupPlotViews);
        this.listenTo(this.subview('rec_offset'), 'error', function(msg) {
            Notifier.error({
                'text': 'msg'
            });
        });
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
    
    setPlotAssignments: function(assignments) {
        this.model.plots.each(function(plot){
            var key = plot.get('key');
            assignments['input[data-key="'+key+'"]'] = key;
        }, this);
    },
    
    render: function() {
        var json = this.model.serialize();
        json['x_formats'] = this.model.x_formats;
        var markup = this.template(json);
        this.$el.html(markup);
        var assignments = {
            '.toggle_tail': 'toggle_tail',
            '.rec_offset': 'rec_offset',
            '.rec_limit': 'rec_limit'
        };
        this.setPlotAssignments(assignments);
        this.assign(assignments);
        
        // tool tips
        this.$('form [title]').tooltip({
            delay: { show: 800, hide: 300 },
            placement: 'left'
        });
        
        
        return this;
    },
    
    template: kt.make(__dirname+'/LineControl.html','_')
    
});
exports = module.exports = Control;