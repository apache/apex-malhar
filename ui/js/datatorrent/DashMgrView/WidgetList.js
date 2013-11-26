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
var Notifier = require('../Notifier');
var BaseView = require('bassview');
var WidgetItem = require('./WidgetListItem');
var WidgetList = BaseView.extend({
    
    initialize: function(options) {
        this.dashes = options.dashes;
        this.curDash = undefined;
        this.updateCurDash();
        this.listenTo(this.dashes, 'change:selected renderWidgetList', function() {
            this.updateCurDash();
        });
    },
    
    updateCurDash: function() {
        if (this.curDash) this.curDash.get('widgets').off('sort', this.renderWidgets, this);
        this.curDash = this.dashes.find(function(dash){ return dash.get('selected') });
        if (this.curDash) {
            var widgets = this.curDash.get('widgets');
            this.listenTo(widgets, 'renderList add', function(){
                this.renderWidgets();
            }, this);
        }
    },
    
    render: function() {
        this.trigger('clean_up');
        var json = {
            dash: this.curDash ? this.curDash.toJSON() : {},
            wClasses: this.collection.toJSON()
        };
        
        this.$el.html(this.template(json));
        this.renderWidgets();
        return this;
    },
    
    renderWidgets: function() {
        this.trigger('clean_up');
        var $ul = this.$('ul.widget-instances');
        // get current dashboard
        var dash = this.curDash;
        if (!dash) return;
        // loop through its widgets
        var widgets = dash.get('widgets');
        widgets.each(function(wDef) {
            // create a widget listitem view
            var view = new WidgetItem({ model: wDef, collection: widgets });
            // listen to clean_up event
            view.listenTo(this, 'clean_up', view.remove);
            // append to the list
            $ul.append(view.render().el);
        }, this);
        // sortable behavior for the list
        $ul.sortable({
            axis: 'y',
            helper: 'clone',
            placeholder: 'item-placeholder',
            stop: function(evt, ui) {
                widgets.order = $ul.sortable('toArray', { attribute: 'data-id' });
                widgets.sort();
                widgets.trigger('sortWidgets', widgets, dash);
            }
        });
        $ul.disableSelection();
    },
    
    events: {
        'click .addWidgetOption': 'addWidget'
    },
    
    addWidget: function(evt){
        evt.preventDefault();
        var wname = $(evt.target).data('wname');
        // check that this widget is available
        var widgetClass = this.collection.get(wname);
        var limit = widgetClass.get('limit')*1;
        if (! widgetClass) return;
        // get current intances in this dash
        var curWidgets = this.curDash.get('widgets');
        // check that there is not a limit restriction on this widget class
        if (limit > 0) {
            // get number of this class currently instantiated
            var count = curWidgets.where({'widget':wname}).length;
            // Check that this count is less than the limit
            if (count >= limit) {
                Notifier.info({
                    'title': 'Only '+limit+' \''+wname+'\' widget(s) can be instantiated at a time.',
                    'text': 'This is usually due to performance reasons or functionality conflict.'
                });
                return;
            }
        }
        // pluck the ids of the widgets
        var ids = curWidgets.pluck('id');
        // generate a new name 'untitled X'
        var defaultId = widgetClass.get('defaultId');
        var counter = 0;
        var newId = defaultId;
        while (ids.indexOf(newId) !== -1) {
            counter++;
            newId = defaultId + ' '.concat(counter);
        }
        // add id to the array, make sort array
        ids.push(newId);
        curWidgets.order = ids;
        // add to the widget collection
        curWidgets.add({
            'widget': wname,
            'id': newId
        });
    },
    
    template: kt.make(__dirname+'/WidgetList.html','_')
    
});
exports = module.exports = WidgetList