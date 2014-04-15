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
var Notifier = require('./Notifier');
var BaseView = require('bassview');
var SettingsView = require('./WidgetSettingsView');
var WidgetCtrl = BaseView.extend({
    
    initialize: function() {
        // Extend events if necessary
        if (this.events !== WidgetCtrl.prototype.events) {
            this.events = _.extend({},WidgetCtrl.prototype.events,this.events);
        }
        
        // Listen for changes to the id
        this.listenTo(this.model, 'change:id', this.render);
    },
    
    events: {
        'click .wi-delete': 'onDeleteClick',
        'click .wi-settings': 'openSettings',
        'click .wi-editName': 'editId',
        'dblclick span.wid': 'editId',
        'blur input.wid': 'saveId',
        'keydown input.wid': 'checkForEnter'
    },
    
    onDeleteClick: function(evt) {
        // Clear the timeout function if there
        if (this.__single_click_timeout) clearTimeout(this.__single_click_timeout);
        
        // check if this is a second click in a dblclick
        if (this.__delete_click_triggered) {
            this.__delete_click_triggered = false;
            this.deleteNoAlert(evt);
            return;
        }
        
        // not a dblclick, set the timeout
        this.__single_click_timeout = setTimeout(function(){
            this.__delete_click_triggered = false;
            this.deleteWithAlert(evt);
        }.bind(this), 200);
        
        // variable to keep track of dblclick/single click state
        this.__delete_click_triggered = true;
    },
    
    deleteWithAlert: function(evt) {
        var c = confirm('Are you sure you want to delete this widget from the dashboard? Note: double-click the "x" icon to suppress this warning.');
        if (c) {
            this.deleteNoAlert();
        }
    },
    
    deleteNoAlert: function() {
        this.collection.remove(this.model);
        this.model.trigger('remove');
    },

    editId: function(evt){

        if (this._editingId) {
            this.saveId(evt);
            return;
        }

        evt.preventDefault();
        evt.stopPropagation();

        // cache the .wid el
        var $wid = this.$('.wid');
        if (!$wid.length) return;
        // get the current id
        var curId = this.model.get('id');
        // create input
        var $input = $('<input type="text" class="wid input-small" value="'+curId+'" />');
        // replace el with the new input
        $wid.replaceWith($input);
        // focus on the input
        $input.focus();

        this._editingId = true;
    },
    
    checkForEnter: function(evt) {
        if (evt.which == 13) this.saveId(evt);
    },

    saveId: _.throttle(function(evt){
        // get val of input
        var newId = evt.target.value;
        
        // check if nothing has been changed
        if (newId == this.model.get('id')) return this.render();
        
        // check the collection for other widgets with this id
        if (this.model.collection) {
            var already = this.model.collection.get(newId);
            if (already) {
                this.render();
                Notifier.error({
                    'title': 'id \''+newId+'\' in use',
                    'text': 'There is already a widget with that id. Ensure that the new name is not used.'
                });
                return;
            }
        }
        
        // set the id to this
        this.model.set('id', newId);

        this._editingId = false;
    }, 100),
    
    openSettings: function(evt) {
        var settings = new SettingsView({
            model: this.model
        });
        var $settings_el = $('<div class="widget-settings modal hide"></div>').appendTo('body');
        
        settings.setElement($settings_el[0]);
        settings.render();
    }
    
});
exports = module.exports = WidgetCtrl;