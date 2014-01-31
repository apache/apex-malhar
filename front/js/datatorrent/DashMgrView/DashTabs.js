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
var BaseView = require('bassview');
var Notifier = require('../Notifier');
var DashActions = BaseView.extend({
    
});

var DashTab = BaseView.extend({
    
    tagName: "li",
    
    template: kt.make(__dirname+'/DashTab.html','_'),
    
    render: function() {
        var json = this.model.toJSON();
        var markup = this.template(json);
        this.$el.html(markup);
        if (json.selected) {
            this.$el.addClass('selected');
        }
        return this;
    },
    
    events: {
        "click label.dash_id": "selectDash",
        "click i.editDashName": "editDashId",
        "blur input.dash_id": "saveDashId",
        "keydown input.dash_id": "checkForEnter",
        "click i.removeDash": "removeDash"
    },
    
    selectDash: function(evt) {
        evt.preventDefault();
        if (this.model.get('selected')) return;
        
        this.$('input[name="curDashRadio"]').prop('checked', true).click();
    },
    
    editDashId: function(evt) {
        console.log('editDashId');
        evt.preventDefault();
        
        if (this.model.get("isDefault")) {
            return Notifier.warning({
                "title":"Default dashboards cannot be renamed",
                "text":"Click the 'create new' button in the dashboards palette to create a custom dashboard using the currently selected one as a template. You may rename custom dashboards by double-clicking the name. "
            });
        }
        // cache the .dash_id el
        var $did = $(evt.target).prev('.dash_id');
        // get the current id
        var curId = this.model.get('dash_id');
        // create input
        var $input = $('<input type="text" class="dash_id input-small" value="'+curId+'" />');
        // replace el with the new input
        $did.replaceWith($input);
        // focus on the input
        $input.focus();
    },
    
    saveDashId: function(evt){
        // get val of input
        var newId = evt.target.value;

        // check if nothing has been changed
        if (newId == this.model.get('dash_id')) return this.render();

        // check the collection for other widgets with this id
        if (this.model.collection) {
            var already = this.model.collection.get(newId);
            if (already) {
                this.render();
                Notifier.error({
                    "title": "dash id '"+newId+"' in use",
                    "text": "There is already a dashboard with that id. Ensure that the new name is not used."
                });
                return;
            }
        }

        // set the id to this
        this.model.set('dash_id', newId);
        this.render();
    },
    
    checkForEnter: function(evt) {
        if (evt.which == 13) this.saveDashId(evt);
    },
    
    removeDash: function(evt) {
        var c = confirm("Are you sure you want to remove this dashboard? This cannot be undone.");
        if (c) {
            var col = this.model.collection;
            var model = this.model;
            if (model.get("selected")) {
                // First select another dashboard
                var curIdx = col.indexOf(model);
                var newId = col.at(--curIdx).get('dash_id');
                col.page.loadDash(newId).trigger("renderDash");
            }
            // remove from collection
            col.remove(model);
            // remove this view
            this.remove();
        }
    }
    
});

var DashTabs = BaseView.extend({
   
    initialize: function(options) {
        this.page = options.page;
        this.listenTo(this.collection, "add change:selected remove", this.render);
    },
    
    // template: kt.make(__dirname+'/DashTabs.html','_'),
    template: '<ul><li class="createDashWrapper"><button class="btn btn-mini createDash"><i class="icon-plus"></i></button></li></ul>',
    
    render: function(){
        this.trigger("clean_up");
        this.$el.html(this.template);
        this.renderDashTabs();
        return this;
    },
    
    renderDashTabs: function() {
        // cache the list element
        var $addBtn = this.$('ul .createDashWrapper');
        // loop through dashes (this.collection)
        this.collection.each(function(dash) {
            // create item view
            var view = new DashTab({ model: dash, collection: this.collection });
            // listen for clean up
            view.listenTo(this, "clean_up", view.remove);
            // add to the list
            view.render().$el.insertBefore($addBtn);
        }, this)
        return this;
    },
    
    events: {
        "click .createDash": "createDash",
        "click input[name='curDashRadio']": "changeDash"
    },
    
    createDash: function(evt){
        evt.preventDefault();
        // get json of current dash
        var json = {
            "widgets": this.collection.find(function(dash){ return dash.get('selected') }).get('widgets').toJSON(),
            "selected": false,
            "isDefault": false,
            "changed": true
        }
        // generate new id for dash
        var dash_id = "Untitled Dash";
        var counter = 0;
        while (this.collection.get(dash_id)) {
            counter++;
            dash_id = "Untitled Dash ".concat(counter);
        }
        // replace new name
        json["dash_id"] = dash_id;
        // add this dash to the dashboards collections
        this.collection.add(json);
        // go to the new dash
        this.page.loadDash(dash_id).trigger("renderDash");
    },
    
    changeDash: function(evt) {
        // Get the value of the checked input
        var checked_id = this.$("input:checked").val();
        // Load this dash and render it
        this.page.loadDash(checked_id).trigger("renderDash");
    }
    
});
exports = module.exports = DashTabs