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
 * TemplateParamsView
 * 
 * View that generates inputs for entering
 * parameters for an alert.
*/
var _ = require('underscore');
var kt = require('knights-templar');
var bassview = require('bassview');
var Bbind = DT.lib.Bbindings;
var TemplateParamsView = bassview.extend({
    
    initialize: function() {
        this.listenTo(this.model, 'change:templateName', this.render);
        this.listenTo(this.collection, 'change:parameters', this.render);
    },
    
    render: function() {
        
        var html, 
            json = { template: false }, 
            assignments = {},
            templateName = this.model.get('templateName'),
            paramsModel = this.model.get('parameters'),
            alertTemplate;
        
        // clear the parameter model on the alert model
        paramsModel.clear();
        
        // trigger removal of any subviews
        this.trigger('clean_up');

        // check if a template name has been specified
        if (templateName) {
            
            // retrieve the template model
            if (alertTemplate = this.collection.get(templateName).toJSON()) {
                
                json.template = alertTemplate;
                var InputClass, enumRegex = /^enum:(.*)$/;
                _.each(alertTemplate.parameters, function(param) {
                    
                    // check for enums, transform those params
                    var matches = enumRegex.exec(param.type);
                    if (matches) {
                        param.options = matches[1].split(',');
                        param.type = 'enum';
                        InputClass = Bbind.select;
                    } else {
                        InputClass = Bbind.text;
                    }
                    // create subview for binding the inputs to the attributes in paramsModel
                    assignments['[name="param_' + param.name + '"]'] = this.subview('param_' + param.name, new InputClass({
                        attr: param.name,
                        model: paramsModel,
                        classElement: function($el) {
                            return $el.parent().parent();
                        },
                        errorClass: 'error'
                    }));
                    
                }, this);
            }
        }
        html = this.template(json);
        this.$el.html(html);
        this.assign(assignments);
        return this;
    },
    
    template: kt.make(__dirname+'/TemplateParamsView.html','_')
    
});
exports = module.exports = TemplateParamsView;