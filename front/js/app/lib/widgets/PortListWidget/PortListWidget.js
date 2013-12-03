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
 * The ports list widget
 * 
*/
var ListWidget = DT.widgets.ListWidget;
var Tabled = DT.lib.Tabled;
var Palette = require('./PortListPalette');
var list_columns = require('./columns');
var Ports = DT.lib.PortCollection;
var PortList = ListWidget.extend({
    
    initialize: function(options) {
        // Call super init
        ListWidget.prototype.initialize.call(this, options);
        
        // injections
        this.dataSource = options.dataSource;
        
        // set up ports collection
        this.ports = new Ports(this.model.get('ports'), { 
            dataSource: this.dataSource,
            appId: this.model.get('appId'),
            operatorId: this.model.get('id')
        });

        // create the tabled view
        this.subview('tabled', new Tabled({
            collection: this.ports,
            columns: list_columns,
            id: 'portlist'+this.compId(),
            save_state: true
        }));
        
        // create the palette view
        this.subview('palette', new Palette({
            collection: this.ports,
            model: this.model,
            dataSource: this.dataSource,
            nav: options.nav
        }));
        
        // listen for port changes
        this.listenTo(this.model, 'change:ports', this.updatePorts);
    },
    
    updatePorts: function() {
        this.ports.set(this.model.get('ports'));
    }
    
});
exports = module.exports = PortList;