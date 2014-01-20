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

var ListWidget = DT.widgets.ListWidget;
var Tabled = DT.lib.Tabled; 
var Palette = require("./AppListPalette");

/**
 * This widget shows a list of running applications
 * in the cluster.
 * 
*/
var ApplistWidget = ListWidget.extend({
    
    initialize: function(options) {
        // Call super init
        ListWidget.prototype.initialize.call(this, options);
        
        // Pick up injections
        this.dataSource = options.dataSource;
        this.apps = options.apps;
        this.nav = options.nav;
        
        // Set up the table
        var columns = require('./columns');
        this.subview("tabled", new Tabled({
            collection:this.apps,
            columns:columns,
            id: "ops.applist."+this.compId(),
            save_state: true,
            row_sorts: ["state","id"],
            max_rows: 20
        }));
        
        // Set up the palette
        this.subview("palette", new Palette({
            collection: this.apps,
            nav: this.nav,
            dataSource: this.dataSource
        }));
    }
    
});
exports = module.exports = ApplistWidget;