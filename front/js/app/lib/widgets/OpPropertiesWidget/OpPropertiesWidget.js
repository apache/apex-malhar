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
 * Alert List Widget.
 */

var _ = require('underscore');
var Backbone = require('backbone');
var Tabled = DT.lib.Tabled;
var columns = require('./columns');
var ListWidget = DT.widgets.ListWidget;
var OpPropertiesModel = DT.lib.OpPropertiesModel;
var LogicalOperatorModel = DT.lib.LogicalOperatorModel;
var OpPropertiesWidget = ListWidget.extend({

    initialize: function(options) {
        ListWidget.prototype.initialize.call(this, options);

        this.collection = new Backbone.Collection();

        // create the tabled view
        this.subview('tabled', new Tabled({
            collection: this.collection,
            columns: columns,
            id: 'proplist' + this.compId(),
            save_state: true,
            max_rows: 10
        }));
        
        // Check if the operator has loaded
        var logicalNameAttr; // Attribute of name of logical operator

        if (this.model instanceof LogicalOperatorModel) {
            logicalNameAttr = 'logicalName';
        } else {
            logicalNameAttr = 'name';
        }

        if (this.model.get(logicalNameAttr)) {
            
            this.setProperties(this.model.get('appId'), this.model.get(logicalNameAttr));
            
        } else {
            
            this.listenToOnce(this.model, 'change:' + logicalNameAttr, function(model, attr) {
                this.setProperties(this.model.get('appId'), attr);
            });
            
        }
    },
    
    setProperties: function(appId, operatorName) {
        var propertiesModel = new OpPropertiesModel({}, {
            appId: appId,
            operatorName: operatorName
        });
        
        this.listenToOnce(propertiesModel, 'sync', this.updateCollection);
        propertiesModel.fetch();
    },

    updateCollection: function(model) {
        _.each(model.toJSON(), function(value, key) {
            var property = {
                key: String(key),
                value: String(value)
            };
            this.collection.add(property);
        }, this);
        this.render();
    }

});
exports = module.exports = OpPropertiesWidget;