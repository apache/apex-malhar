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
 * List Widget Palette
 * 
 * The base class for palettes 
 * used in list widgets.
*/
var kt = require('knights-templar');
var _ = require('underscore');
var BaseView = require('bassview');
var ListPalette = BaseView.extend({
    
    initialize: function(options) {
        
        if (!this.collection) {
            throw new TypeError('ListPalette requires a collection');
        }
        
        // look for things to attach directly to this instance
        _.each(['dataSource', 'nav'], function(prop) {
            if (options[prop]) {
                this[prop] = options[prop];
            }
        }, this);
        
        this.listenTo(this.collection, 'change_selected remove', this.render);
        
    },
    
    getSelected: function(pojo) {
        var selected = this.collection.filter(function(row) {
            return row.selected === true;
        });
        
        if (pojo) {
            return _.map(selected, function(row) {
                return row.toJSON();
            });
        }
        return selected;
    },
    
    getTemplateData: function() {
        return {
            selected: this.getSelected(true)
        }
    },
    
    render: function() {
        var json = this.getTemplateData();
        var html = this.template(json);
        this.$el.html(html);
        return this;
    },
    
    template: kt.make(__dirname+'/ListPalette.html','_'),
    
    deselectAll: function() {
        var changed = false;
        this.collection.each(function(item) {
            if (item.selected) {
                changed = true;
                item.selected = false;
            }
        })
        if (changed) {
            this.collection.trigger('change_selected');
        }
    }
    
});

exports = module.exports = ListPalette;