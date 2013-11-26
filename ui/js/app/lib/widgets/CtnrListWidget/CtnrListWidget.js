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
 * The operator list widget
 * 
*/
var ListWidget = DT.widgets.ListWidget;
var Tabled = DT.lib.Tabled; 
var Palette = require('./CtnrListPalette');
var list_columns = require('./columns');
var CtnrListWidget = ListWidget.extend({
    
    initialize: function(options) {
        // Call super init
        ListWidget.prototype.initialize.call(this, options);
        
        // Set the injected stuff
        this.dataSource = options.dataSource;
        this.ctnrs = options.containers;
        this.instance = options.instance;
        this.nav = options.nav;
        
        // Set up the table
        this.subview('tabled', new Tabled({
            collection:this.ctnrs,
            columns:list_columns,
            id: 'ops.apps.app.ctnrs'+this.compId(),
            save_state: true,
            row_sorts: ['id'],
            max_rows: 10
        }));
        
        // Set up the palette
        this.subview('palette', new Palette({
            model: this.instance,
            collection: this.ctnrs,
            nav: this.nav,
            dataSource: this.dataSource
        }));
    },
    
    remove: function() {
        this.ctnrs.each(function(ctnr){
            delete ctnr.selected;
        });
        ListWidget.prototype.remove.call(this);
    }
    
});
exports = module.exports = CtnrListWidget;