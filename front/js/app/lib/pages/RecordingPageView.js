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
 * Recording Page
 * 
 * 
 * Holds the tuple viewer and may be used for charting.
 * 
*/

var _ = require('underscore');
var Notifier = DT.lib.Notifier;
var BasePageView = DT.lib.BasePageView;
var Recording = DT.lib.RecordingModel;

// widgets
var TupleViewerWidget = require('../widgets/TupleViewerWidget');

var RecordingPageView = BasePageView.extend({
    
    pageName:'RecordingPageView',
    
    useDashMgr: true,
    
    // Define the default dashboard configurations
    defaultDashes: [
        {
            dash_id: 'default',
            widgets: [
                { widget: 'tupleviewer', id: 'tuple viewer' }
            ]
        }
    ],
    
    // Page set-up
    initialize: function(options) {
        // Super
        BasePageView.prototype.initialize.call(this,options);
        
        this.defineWidgets([
            {
                name: 'tupleviewer',
                defaultId: 'tuple viewer',
                view: TupleViewerWidget,
                inject: {
                    dataSource: this.dataSource,
                    args: options.pageParams
                }
            }
        ]);
        
        // Load up previous or 'default' dashboard
        this.loadDashboards('default');
        
        // Listen for application id change
        this.on('change:url_args', function() {
            this.render();
        });
    }
    
});

exports = module.exports = RecordingPageView;