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
var BaseView = require('../WidgetCtrlView');
var WidgetItem = BaseView.extend({
    
    initialize: function(options) {
        BaseView.prototype.initialize.call(this, options);
        
        this.listenTo(this.model, 'remove', this.remove);
    },
    
    tagName: 'li',

    render: function() {
        var json = this.model.toJSON();
        var markup = this.template(json);
        this.$el.html(markup);
        this.$('span.wid').tooltip({
            show: {
                delay: 500,
                duration: 250,
                effect: 'fadeIn',
                easing: 'easeOutExpo'
            }
        });
        this.$el.attr('data-id', json.id);
        return this;
    },
    
    events: {
        'mouseenter': 'highlightWidget',
        'mouseleave': 'unhighlightWidget'
    },
    
    highlightWidget: function() {
        this.model.trigger('hover_on_widget', true);
    },
    
    unhighlightWidget: function() {
        this.model.trigger('hover_on_widget', false);
    },
    
    template: kt.make(__dirname+'/WidgetListItem.html','_')
    
});
exports = module.exports = WidgetItem;