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

var Tabled = DT.lib.Tabled;
var columns = require('./columns');
var Palette = require('./Palette');
var ListWidget = DT.widgets.ListWidget;
var AlertCollection = DT.lib.AlertCollection;

var AlertListWidget = ListWidget.extend({

    initialize: function(options) {
        ListWidget.prototype.initialize.call(this, options);

        this.collection = new AlertCollection([], {
            appId: options.appId
        });

        // create the tabled view
        this.subview('tabled', new Tabled({
            collection: this.collection,
            columns: columns,
            id: 'alertlist' + this.compId(),
            save_state: true,
            max_rows: 10
        }));

        // create the palette view
        this.subview('palette', new Palette({
            collection: this.collection,
            appId: options.appId,
            nav: options.nav
        }));

        this.listenTo(this.collection, 'sync remove', this.render);
        this.collection.fetch();
    }

});
exports = module.exports = AlertListWidget