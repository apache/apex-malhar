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
 * Filter Detail View
 * 
 * This view will contain the 
 * inputs for a given operator class
*/
var _ = require('underscore');
var Backbone = require('backbone');
var Bbindings = DT.lib.Bbindings;
var bassview = require('bassview');
var kt = require('knights-templar');
var ClassPropertiesView = bassview.extend({
    
    initialize: function(options) {
        window.testClassProp = this;
        
        // listen for changes to the properties
        this.listenTo(this.model, 'change:properties', this._onProperties);
        
        // init properties that may already be there
        this._onProperties();
    },
    
    _onProperties: function() {
        // create a new model for property values
        var propertiesModel = new Backbone.Model({});
        
        // remove all property subviews from before
        this.trigger('clean_up');
        
        // reset the propertyValues model on the operator class model
        this.model.set('propertyValues', propertiesModel)
        
        // reset the assignments hash
        this.assignments = {};
        
        // create subviews, assignments, and validation for each property
        _.each(this.model.get('properties'), function(prop) {
            
            // create the subview
            var view, 
                type = prop['class'], 
                name = prop['name'],
                BBClass;
            
            // choose the right bbinding class for the type
            if ( type === 'boolean' ) {
                BBClass = Bbindings.checkbox;
            }
            else if (['long','int'].indexOf(type) !== -1) {
                BBClass = Bbindings.textint;
            }
            else if (type === 'double') {
                BBClass = Bbindings.textfloat;
            }
            else {
                BBClass = Bbindings.text;
            }
            
            this.subview(name, new BBClass({
                model: propertiesModel,
                attr: name,
                classElement: function($el) {
                    return $el.parent();
                },
                errorClass: 'error'
            }));
            this.assignments['[data-property="' + name + '"]'] = name;
            
        }, this);
        
        this.render();
    },
    
    render: function() {
        var json = this.model.toJSON();
        var html = this.template(json);
        this.$el.html(html);
        this.assign(this.assignments);
        return this;
    },
    
    template: kt.make(__dirname+'/ClassPropertiesView.html','_')
    
});

exports = module.exports = ClassPropertiesView;