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
 * Tuple Viewer Widget
*/
var kt = require('knights-templar');
var _ = require('underscore');
var Backbone = require('backbone');
var BaseView = DT.widgets.Widget;
var BigInteger = require('jsbn');
var RecordingModel = require('./lib/TvModel');
var RecView = require('./lib/TvView');

var TupleViewerWidget = BaseView.extend({
    
    initialize: function(options) {
        // Call super init
        BaseView.prototype.initialize.call(this, options);
        
        // Data source
        this.dataSource = options.dataSource;
        
        // Url args: [ appId , operatorId , startTime ]
        var args = options.args;
        
        // Create the model and view
        this.recordingModel = new RecordingModel({
            appId: args.appId,
            operatorId: args.operatorId,
            startTime: args.startTime
        },{
            dataSource: this.dataSource
        });
        
        this.subview('recView', new RecView({
            model: this.recordingModel
        }));
        
        // Update the model
        this.recordingModel.fetch();
    },
    
    html: function() {
        return this.template({});
    },
    
    assignments: {
        '.viewer-container': 'recView'
    },
    
    template: kt.make(__dirname+'/TupleViewerWidget.html','_')
    
});

exports = module.exports = TupleViewerWidget;