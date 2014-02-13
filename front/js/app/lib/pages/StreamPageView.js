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
 * Stream Page
 * 
 * 
 * Displays information about a stream on a running application instance.
 * 
*/

var _ = require('underscore');
var Notifier = DT.lib.Notifier;
var Base = DT.lib.BasePageView;
var ApplicationModel = DT.lib.ApplicationModel;
var StreamModel = DT.lib.StreamModel;

// widget
var StreamInfoWidget = require('../widgets/StreamInfoWidget');
var StreamPortListWidget = require('../widgets/StreamPortListWidget');

var StreamPage = Base.extend({
    
    pageName:'StreamPageView',
    
    useDashMgr: true,
    
    // Define the default dashboard configurations
    defaultDashes: [
        {
            dash_id: 'default',
            widgets: [
                { widget: 'StreamInfo', id: 'stream info' },
                { widget: 'SourcePorts', id: 'sources', width: 50 },
                { widget: 'SinkPorts', id: 'sinks', width: 50 }
            ]
        }
    ],
    
    // Page set-up
    initialize: function(options) {
        // Super
        Base.prototype.initialize.call(this,options);
        
        // Get initial args
        var url_args = this.app.nav.get('url_args');
        
        // Create the app model
        this.instance = new ApplicationModel({
            id: url_args[0]
        },{
            dataSource: this.dataSource
        });
        this.instance.setOperators([])
        this.instance.setContainers([])
        this.instance.fetch({
            async: false
        });
        
        // Only subscribe to operators
        this.instance.operators.subscribe();
        
        // Create the stream model
        this.model = new StreamModel(this.instance.getStream(url_args[1]), {
            dataSource: this.dataSource,
            application: this.instance
        })
            .setSourcePorts([])
            .setSinkPorts([])
            .subscribeToUpdates();
        
        
        this.defineWidgets([
            { name: 'StreamInfo', defaultId: 'stream info', view: StreamInfoWidget, inject: {
                dataSource: this.dataSource,
                model: this.model,
                instance: this.instance
            }},
            { name: 'SourcePorts', defaultId: 'sources', view: StreamPortListWidget, inject: {
                model: this.model,
                collection: this.model.sourcePorts,
                title: 'Source Ports',
                portType: 'source'
            }},
            { name: 'SinkPorts', defaultId: 'sinks', view: StreamPortListWidget, inject: {
                model: this.model,
                collection: this.model.sinkPorts,
                title: 'Sink Ports',
                portType: 'sink'
            }}
        ]);
        
        this.loadDashboards('default');
    },
    
    cleanUp: function() {
        this.instance.cleanUp();
        this.model.cleanUp();
        Base.prototype.cleanUp.call(this);
    }
    
    
});

exports = module.exports = StreamPage;