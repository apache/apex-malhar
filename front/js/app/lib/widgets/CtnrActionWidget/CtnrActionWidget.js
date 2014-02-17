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
 * OpActionWidget
 * 
 * Executes various actions for an operator
*/

var _ = require('underscore');
var kt = require('knights-templar');
var BaseView = DT.widgets.Widget;
var CtnrActionWidget = BaseView.extend({

    className: 'visible-overflow',

    initialize: function() {
        BaseView.prototype.initialize.apply(this, arguments);
        this.listenTo(this.model, 'change:containerLogsUrl', this.renderContent);
    },
    
    events: {
        'click .killCtnr': 'killCtnr'
    },

    killCtnr: function() {
        this.$('.killCtnr').prop('disabled', true);
        this.model.kill();
    },
    
    template: kt.make(__dirname+'/CtnrActionWidget.html','_')

});
exports = module.exports = CtnrActionWidget;
