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
 * Action Sub View
 * 
 * This section of the add alert page is where the user chooses
 * which filter class to use for the alert condition.
*/
var bassview = require('./BaseSubView');
var ActionSubView = bassview.extend({
    
    initialize: function(options) {
        
        // Set subviews for properties div and ports div
        this.subview('propertyDefinitions', new this.ClassPropertiesView({
            model: this.model.get('actions')[options.index]
        }));
        // Listen to reset event
        this.listenTo(this.collection, 'reset', this.render);
    },
    
    onClassChange: function(e) {
        var className = this.$('.classSelect').val();
        var classModel = this.model.get('actions')[this.options.index];
        // set the new class name, clear out other attributes
        classModel.set({
            'name': className,
            'properties': [],
            'inputPorts': [],
            'outputPorts': []
        }); 
        classModel.load(); // load the class model information
    },
    
    classType: 'action'
    
});
exports = module.exports = ActionSubView;