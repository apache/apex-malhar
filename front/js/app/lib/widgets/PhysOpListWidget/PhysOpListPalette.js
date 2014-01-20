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
 * Contextual Palette for Operator list
 * 
 * collection: operators
 * model: application
*/
var _ = require('underscore');
var BaseView = DT.lib.ListPalette;
var kt = require('knights-templar');
var Palette = BaseView.extend({
    
    initialize: function(options) {
        this.appId = options.appId;
        this.nav = options.nav;
        this.dataSource = options.dataSource;
        this.listenTo(this.collection, "change_selected", this.render);
        this.listenTo(this.collection, "change:recordingStartTime", this.render);
    },
    
    events: {
        "click .inspectOperator": "goToSelectedOp",
        "click .startOpRecording": "startOpRecording",
        "click .stopOpRecording": "stopOpRecording"
    },
    
    goToSelectedOp: function(evt) {
        var operatorId = this.collection.find(function(model){return model.selected}).get("id");
        var appId = this.appId;
        this.nav.go('ops/apps/'+appId+'/operators/'+operatorId, {trigger: true} );
    },
    
    startOpRecording: function(evt) {
        this.$('.startOpRecording').prop('disabled', true);
        var operatorId = this.collection.find(function(model){return model.selected}).get("id");
        var appId = this.appId;
        this.dataSource.startOpRecording({
            appId: appId,
            operatorId: operatorId
        });
    },
    
    stopOpRecording: function(evt) {
        this.$('.stopOpRecording').prop('disabled', true);
        var operator = this.collection.find(function(model){
            return model.selected;
        });
        var operatorId = operator.get('id');
        var appId = this.appId;
        
        
        // Get the recording
        this.dataSource.stopOpRecording({
            appId: appId, 
            operatorId: operatorId
        });
        
    },
    
    template: kt.make(__dirname+'/PhysOpListPalette.html', '_')
    
});

exports = module.exports = Palette