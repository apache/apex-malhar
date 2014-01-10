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
var OpActionWidget = BaseView.extend({

    initialize:function(options) {
        BaseView.prototype.initialize.call(this,options);
        this.dataSource = options.dataSource;
        this.listenTo(this.model, 'change:recordingStartTime change:ports', this.render);
    },
    
    events: {
        'click .startRecording': 'startRecording',
        'click .stopRecording':  'stopRecording'
    },
    
    startRecording: function(evt) {
        this.$('.startRecording').prop('disabled', true);
        var operatorId = this.model.get("id");
        var appId = this.model.get('appId');
        this.dataSource.startOpRecording({
            appId: appId,
            operatorId: operatorId
        });
    },
    
    stopRecording: function(evt) {
        this.$('.stopRecording').prop('disabled', true);
        var operator = this.model;
        var operatorId = operator.get('id');
        var appId = operator.get('appId');
        
        // Get the recording
        this.dataSource.stopOpRecording({
            appId: appId, 
            operatorId: operatorId
        });
    },
    
    template: kt.make(__dirname+'/OpActionWidget.html','_')
});
exports = module.exports = OpActionWidget;
