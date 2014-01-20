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
var _ = require('underscore'), Backbone = require('backbone');
var BaseView = require('./base');
var Tabled = require('../../../tabled');
var Table = BaseView.extend({
    
    render: function() {
        
        this.$el.html('');
        
        // create bc of data
        this.bcData = new Backbone.Collection([]);
        
        // create tabled
        this.subview('tabled', new Tabled({
            columns: [
                { id: this.idAttribute , label: this.idAttribute , key: this.idAttribute, filter: 'like', sort: 'string', format: function(val, row) {
                    return '<span class="color-box" style="background-color: ' + row.get('_topn_color_') + ';"></span> ' + val ;
                } },
                { id: this.valAttribute, label: this.valAttribute, key: this.valAttribute, filter: 'number', sort: 'number', sort_value: 'd' }
            ],
            collection: this.bcData,
            save_state: true,
            el: this.el,
            id: 'topn.table.' + this.widgetView.compId(),
            col_sorts: [this.valAttribute]
        }).render());
        
        this.onResize();
        
        return this;
        
    },
    
    onData: function(data) {
        BaseView.prototype.onData.call(this, data);
        this.bcData.set(data);
    },
    
    onResize: function() {
        this.subview('tabled').resizeTableToCtnr();
    }
    
});
exports = module.exports = Table;