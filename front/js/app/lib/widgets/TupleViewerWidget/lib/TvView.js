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
 * Main tv view
 * 
 * 
*/
var _ = require('underscore');
var kt = require('knights-templar');
var BaseView = require('bassview');
var HeaderView = require('./HeaderView');
var BodyView = require('./BodyView');
var TvView = BaseView.extend({
    
    initialize:function(){
        
        // Listen for lazy load or name change
        // this.listenTo(this.model, 'change:recordingName', this.render);
        this.listenTo(this.model, 'change:startTime change:totalTuples', this.render);
        
        // Set up subviews
        this.subview('header', new HeaderView({
            model: this.model
        }));
        this.subview('body', new BodyView({
            model: this.model
        }));
    },
    
    render: function(){
        var json = this.model.serialize(true);
        var markup = this.template(json);
        this.$el.html(markup);
        this.assign({
            '.viewer-header' : 'header',
            '.viewer-body'   : 'body'
        });
        return this;
    },
    
    events: {
        "keydown": "handleKeyPress"
    },
    
    handleKeyPress: function(e) {
        switch(e.which) {
            case 40: // down
                e.preventDefault();
                var distance = e.shiftKey ? '10' : '1';
                this.model.shiftSelected(distance);
            break;
            case 38: // up
                e.preventDefault();
                var distance = e.shiftKey ? '-10' : '-1';
                this.model.shiftSelected(distance);
            break;
            default:
                return true;
            break;
        }
        this.model.trigger('update_console');
    },
    
    template: kt.make(__dirname+'/TvView.html','_')
    
});

exports = module.exports = TvView;