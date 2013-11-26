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
 * TemplateSelectView
 * 
 * View for the dropdown menu that holds
 * the alert template options to choose from.
*/

var _ = require('underscore');
var kt = require('knights-templar');
var Bbind = DT.lib.Bbindings;
var bassview = require('bassview');
var TemplateSelectView = bassview.extend({
    
    initialize:function() {
        
        // set the select subview
        this.subview('select', new Bbind.select({
            model: this.model,
            attr: 'templateName',
            classElement: function($el) {
                return $el.parent();
            },
            errorClass: 'error',
            setAnyway: true
        }));
        
        // listen to collection reset for render
        this.listenTo(this.collection, 'reset', this.render);
    },
    
    render: function() {
        var json, html;
        json = {
            templates: this.collection.toJSON(),
            alert: this.model.toJSON()
        };
        html = this.template(json);
        this.$el.html(html);
        this.assign('#templateName', 'select');
        return this;
    },
    
    template: kt.make(__dirname+'/TemplateSelectView.html','_')
    
});
exports = module.exports = TemplateSelectView;