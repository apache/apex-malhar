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
 * Instance Action Widget
 * 
 * This widget is where an application instance 
 * can be killed or an alert can be set, etc.
*/

var _ = require('underscore');
var kt = require('knights-templar');
var BaseView = DT.widgets.Widget;
var InstanceActionWidget = BaseView.extend({

    initialize:function(options) {
        BaseView.prototype.initialize.call(this,options);
        this.dataSource = options.dataSource;
        this.listenTo(this.model, 'change:state', this.render);
    },

    html: function() {
        var html, json = this.model.toJSON();
        var html = this.template(json);
        return html;
    },
    
    events: {
        'click .killApplication': 'killApplication',
        'click .shutdownApplication': 'shutdownApplication'
    },
    
    killApplication: function(evt) {
        this.model.kill(this.dataSource);
    },
    
    shutdownApplication: function(evt) {
        this.model.shutdown(this.dataSource);
    },
    
    template: kt.make(__dirname+'/InstanceActionWidget.html','_')
    
});
exports = module.exports = InstanceActionWidget;