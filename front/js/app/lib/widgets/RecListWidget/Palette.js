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
var _ = require('underscore');
var Notifier = DT.lib.Notifier;
var kt = require('knights-templar');
var BaseView = require('bassview');
var Palette = BaseView.extend({
    
    initialize: function(options) {
        this.dataSource = options.dataSource;
        this.nav = options.nav;
        this.listenTo(this.collection, "change_selected change", this.render );
    },

    render: function() {
        
        var selected = this.getSelected();
        
        var json = {
            one: selected.length === 1,
            none: selected.length === 0,
            many: selected.length > 1
        }
        
        if (json.one) {
            _.extend(json, selected[0].toJSON() );
        }
        
        this.$el.html(this.template(json));
        
        return this;
        
    },
    
    events: {
        'click .refreshRecordings': 'refreshRecordings',
        'click .stopOpRecording': 'stopRecording',
        'click .inspectRecording': 'inspectRecording'
    },
    
    refreshRecordings: function() {
        var done = _.bind(function() {
            if (this.collection.length === 0) {
                Notifier.warning({
                    'title': 'No recordings found',
                    'text': 'If you just started a recording, wait a moment and try refreshing again.'
                });
            }
        }, this);
        this.collection.fetch(done);
        LOG(1, "refreshing reclist");
        
    },
    
    stopRecording: function(evt) {
        var $btn = $(evt.target);
        $btn.prop('disabled', true);
        var operatorId = $btn.data('operatorid');
        var appId = $btn.data('appid');

        this.dataSource.stopOpRecording({
            appId: appId,
            operatorId: operatorId,
            success: _.bind(function() {
                Notifier.success({
                    'title': 'Request to stop recording received',
                    'text': 'The server has received your request. It may take a few moments to reflect recording status. Use the \'refresh list\' button to view changes. If the recording does not stop, this may be an issue with the server.'
                });
            },this)
        });
    },
    
    getSelected: function(){
        return this.collection.filter(function(model){
            return model.selected
        });
    },
    
    inspectRecording: function(evt) {
        var row = this.getSelected()[0];
        var url = 'ops/apps/'+row.get('appId')+'/operators/'+row.get('operatorId')+'/recordings/'+row.get('startTime');
        this.nav.go(url);
    },
    
    template: kt.make(__dirname+'/Palette.html','_')
    
});
exports = module.exports = Palette