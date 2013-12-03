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
 * Recording list for operators
 * 
*/
var _ = require('underscore'), Backbone = require('backbone');
var kt = require('knights-templar');
var ListWidget = DT.widgets.ListWidget;
var Tabled = DT.lib.Tabled;
var Palette = require('./Palette');
var Recordings = DT.lib.RecordingCollection;
var list_columns = require('./columns');
var RecTable = ListWidget.extend({
    
    initialize: function(options) {
        // Call super init
        ListWidget.prototype.initialize.call(this, options);
        
        // Injections
        this.dataSource = options.dataSource;
        this.nav = options.nav;
        
        
        // Set up recordings collection
        this.recordings = new Recordings([], {
            appId: options.pageParams.appId,
            operatorId: options.pageParams.operatorId
        });
        this.recordings.fetch();
        
        // Set up subviews
        this.subview("tabled", new Tabled({
            columns: list_columns,
            collection: this.recordings, 
            id: 'reclist.'+this.compId(),
            save_state: true
        }));
        
        this.subview("palette", new Palette({
            collection: this.recordings,
            dataSource: this.dataSource,
            nav: this.nav
        }));
    },
    
    listTitle: 'Recordings Table'
    
});
exports = module.exports = RecTable;