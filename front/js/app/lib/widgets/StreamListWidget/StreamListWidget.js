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
 * StreamListWidget
 * 
 * Lists the streams for an application
*/

var _ = require('underscore');
var Tabled = DT.lib.Tabled;
var Palette = require('./StreamListPalette');
var kt = require('knights-templar');
var ListWidget = DT.widgets.ListWidget;
var list_columns = require('./columns');
var Streams = DT.lib.StreamCollection;
var StreamListWidget = ListWidget.extend({

    initialize:function(options) {
        
        // Call super init
        ListWidget.prototype.initialize.call(this, options);
        
        // injections
        this.dataSource = options.dataSource;
        
        // set up ports collection
        this.streams = new Streams(this.model.getStreams());
        this.listenTo(this.model, 'change:logicalPlan', function() {
            var streams = this.model.getStreams();
            this.streams.set(streams);
        });

        // create the tabled view
        this.subview('tabled', new Tabled({
            collection: this.streams,
            columns: list_columns,
            id: 'streamlist'+this.compId(),
            row_sorts: ['name'],
            save_state: true
        }));

        
        // create the palette view
        this.subview('palette', new Palette({
            collection: this.streams,
            model: this.model,
            dataSource: this.dataSource,
            pageParams: options.pageParams,
            nav: options.nav
        }));
        
    }
});
exports = module.exports = StreamListWidget;
