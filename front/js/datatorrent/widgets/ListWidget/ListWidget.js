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
 * A base class for list views
*/
var _ = require('underscore');
var kt = require('knights-templar');
var BaseView = require('../../WidgetView');
var ListWidget = BaseView.extend({

    onResize: function() {
        this.subview('tabled').resizeTableToCtnr();
    },

    render: function() {
        var tabled = this.subview('tabled');
        if (!tabled.state('column_widths')) {
            setTimeout(function() {
                tabled.resizeTableToCtnr();
            }, 100);
        }

        var result = BaseView.prototype.render.call(this);

        if ( tabled.collection._fetching ) {
            tabled.setLoading();
        }

        return result;
    },
    
    assignments: function() {
        var assignments = {
            '.table-target': 'tabled'
        }
        
        if (this.subview('palette')) {
            assignments['.palette-target'] = 'palette';
        }
        
        return assignments;
    },
    
    template: kt.make(__dirname+'/ListWidget.html','_'),
    
});
exports = module.exports = ListWidget;