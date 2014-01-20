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
 * JarAppsWidget
 * 
 * Description of widget.
 *
*/

var _ = require('underscore');
var Tabled = DT.lib.Tabled;
var kt = require('knights-templar');
var BaseView = DT.widgets.ListWidget;
var JarAppCollection = DT.lib.JarAppCollection;
var columns = require('./columns');
var Palette = require('./JarAppsPalette');

// class definition
var JarAppsWidget = BaseView.extend({
    
    initialize: function(options) {
        
        BaseView.prototype.initialize.call(this, options);
        
        // Create a collection for applications in this jar
        this.collection = new JarAppCollection([], {
            fileName: options.fileName
        });
        this.collection.fetch();
        
        // Set up the table
        this.subview('tabled', new Tabled({
            collection: this.collection,
            columns: columns,
            id: 'dev.jarappslist.'+this.compId(),
            save_state: true,
            row_sorts: ['name']
        }));
        
        // Set up the palette
        this.subview('palette', new Palette({
            collection: this.collection,
            nav: options.nav
        }));
        
    }
    
});

exports = module.exports = JarAppsWidget;