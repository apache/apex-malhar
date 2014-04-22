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
 * LogicalOpListWidget
 * 
 * Description of widget.
 *
*/

var _ = require('underscore');
var Tabled = DT.lib.Tabled; 
var kt = require('knights-templar');
var BaseView = DT.widgets.ListWidget;
var Palette = require('./LogicalOpListPalette');
var columns = require('./columns');
var LogicalOperatorCollection = DT.lib.LogicalOperatorCollection;

// class definition
var LogicalOpListWidget = BaseView.extend({
    
    initialize: function(options) {
        
        BaseView.prototype.initialize.call(this, options);

        // Set the dataSource and nav
        this.dataSource = options.dataSource;
        this.nav = options.nav;

        if (!this.collection) {
            // Create logical operator collection
            this.collection = new LogicalOperatorCollection([], {
                appId: options.pageParams.appId,
                dataSource: this.dataSource
            });
            this.collection.fetch();
            this.collection.subscribe();    
        }
        
        // listeners, subviews, etc.
        this.subview('tabled', new Tabled({
            columns: columns,
            collection: this.collection,
            id: this.compId() + '.logicalOpList',
            save_state: true,
            row_sorts: ['logicalName']
        }));

        this.subview('palette', new Palette({
            collection: this.collection,
            nav: this.nav,
            appId: options.pageParams.appId
        }));
    },

    remove: function() {
        BaseView.prototype.remove.apply(this, arguments);
    }
    
});

exports = module.exports = LogicalOpListWidget;