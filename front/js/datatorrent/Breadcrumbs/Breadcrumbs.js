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
 * Breadcrumbs View
 * 
 * Renders out a list of links up to the current page
*/
var _ = require('underscore'), Backbone = require('backbone');
var kt = require('knights-templar');
var BaseView = require('bassview');

var Model = Backbone.Model.extend({
    
});

var Collection = Backbone.Collection.extend({
    
    initialize: function(models, options){
        this.nav = options.nav;
        this.loader = options.loader;
    },
    
    model: Model,
    
    serialize: function(){
        var json = this.toJSON();
        var newCrumbs = [];
        _.each(json, function(val, key){
            
            var crumb = $.extend(true, {}, val);
            var argv = this.nav.get("url_args").slice();
            argv.unshift(this.loader.loaded_page);
            
            if (typeof crumb.name === "function") {
                crumb.name = crumb.name.apply(this.loader, argv);
            }

            if (typeof crumb.href === "function") {
                crumb.href = crumb.href.apply(this.loader, argv);
            }
            
            newCrumbs.push(crumb);
            
        }, this);
        return newCrumbs;
    }
    
});



var ColView = BaseView.extend({
    
    initialize: function() {
        this.listenTo(this.model, "change:url_args", this.render);
    },
    
    render: function() {
        var markup = this.template({
            crumbs: this.collection.serialize()
        });
        
        this.$el.html(markup);
        
        return this;
        
    },

    template: kt.make(__dirname+'/Breadcrumbs.html','_')
    
});
exports.colview = ColView;
exports.collection = Collection;
exports.model = Model;