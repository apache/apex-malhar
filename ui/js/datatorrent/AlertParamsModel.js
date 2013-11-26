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
var Backbone = require('backbone');

var AlertParamsModel = Backbone.Model.extend({
    
    validate: function(attrs) {
        // map to hold invalid messages
        var invalid = {}, template_params;
        
        // only continue if a template has been set
        if (!this.template) return;
        
        // Set the parameters
        template_params = this.template.get('parameters');
        
        // get all param definition objects from this.template
        if ( template_params instanceof Array && template_params.length) {
            
            // loop through
            _.each(template_params, function(param_def) {
                
                // Store name and current value
                var name = param_def.name,
                    value = attrs[name];
                
                // if undefined, check for optional
                if ( !value ) {
                    if ( !param_def.optional ) {
                        invalid[name] = 'The ' + name + ' parameter is required.';
                    }
                    return; // either way, we are done
                }
            
                // do type-specific checking
                switch(param_def.type) {
                    
                    // TODO: type-specific checking
                    
                    
                    default:
                        
                        // do nothing
                        
                    break;
                    
                }
                
            }, this);
        }
        
        if (! _.isEmpty(invalid) ) {
            return invalid;
        }
    },
    
    setTemplate: function(template) {
        this.template = template;
    }
    
});

exports = module.exports = AlertParamsModel;