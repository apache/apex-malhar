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
 * The stream ports list widget
 * 
*/
var ListWidget = DT.widgets.ListWidget;
var Tabled = DT.lib.Tabled;
var list_columns = require('./columns');
var StreamPortListWidget = ListWidget.extend({
    
    initialize: function(options) {
        // Call super init
        ListWidget.prototype.initialize.call(this, options);
        
        // injections
        var portType = options.portType;
        this.listTitle = options.title;
        
        
        // create the tabled view
        this.subview('tabled', new Tabled({
            collection: this.collection,
            columns: list_columns(portType),
            id: 'stream' + portType + 'portlist'+this.compId(),
            save_state: true,
            sort: ['portName']
        }));
        
        // create the palette view
        // this.subview('palette', new Palette({
        //     collection: this.ports,
        //     model: this.model,
        //     dataSource: this.dataSource
        // }));
        
        // listen for port changes
        this.listenTo(this.model, 'change:inputPorts change:outputPorts', this.updatePorts);
    }
    
});
exports = module.exports = StreamPortListWidget;