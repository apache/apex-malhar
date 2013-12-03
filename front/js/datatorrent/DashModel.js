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
var Backbone = require('backbone');
var WidgetDefCollection = require('./WidgetDefCollection');
var DashModel = Backbone.Model.extend({
    
    initialize: function(attributes, options) {
        // Make widgets a collection
        this.set(
            { 'widgets': new WidgetDefCollection(attributes.widgets || []) },
            { silent: true }
        );
        var widgets = this.get('widgets');
        this.listenTo(widgets, 'sort remove add change', function() {
            this.set('changed', true);
        });
    },
    
    idAttribute: 'dash_id',
    
    defaults: {
        'dash_id': '',
        'widgets': undefined,
        'selected': false,
        'isDefault': false,
        'changed': false
    },
    
    serialize: function() {
        var json = this.toJSON();
        json.widgets = json.widgets.toJSON();
        return json;
    },
    
    triggerResize: function() {
        var widgets = this.get('widgets');
        if (!widgets.length) return;
        widgets.each(function(widget){
            widget.trigger('resize');
        });
    }
    
});
exports = module.exports = DashModel;