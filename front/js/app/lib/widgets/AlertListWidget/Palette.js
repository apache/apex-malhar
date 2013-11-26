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
 * Palette View for Alert List.
*/
var _ = require('underscore');
var kt = require('knights-templar');
var BaseView = require('bassview');
var Palette = BaseView.extend({

    template: kt.make(__dirname + '/Palette.html','_'),

    events: {
        'click .alert-list-add': 'addAlert',
        'click .alert-list-delete': 'deleteAlert',
        'click .alert-list-refresh': 'refreshList'
    },

    initialize: function(options) {
        this.appId = options.appId;
        this.nav = options.nav;
        this.listenTo(this.collection, 'change_selected remove', this.render);
    },
    
    render: function() {
        var selected = this.getSelected();

        var vo = {
            one: selected.length === 1
        }

        this.$el.html(this.template(vo));
    },

    addAlert: function() {
        var url = 'ops/apps/' + this.appId + '/add_alert';
        this.nav.go(url);
    },

    deleteAlert: function() {
        var selected = this.getSelected();
        if (selected.length === 1) {
            var alertModel = selected[0];
            alertModel.delete();
        }
    },

    refreshList: function() {
        this.collection.load();
    },

    getSelected: function() {
        return this.collection.filter(function(model){
            return model.selected
        });
    }
});
exports = module.exports = Palette;