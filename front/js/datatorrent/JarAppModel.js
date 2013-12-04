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
 * Jar Application Model
 * 
 * Represents an Application that is in a jar file.
 * 
*/
var _ = require('underscore');
var templates = require('./templates');
var Notifier = require('./Notifier');
var BaseModel = require('./BaseModel');
var LogicalPlan = require('./LogicalPlanModel');

// Set properties pop-up
var LaunchJarWithPropsView = require('./LaunchJarWithPropsView');

var JarAppModel = BaseModel.extend({

    debugName: 'jar application',
    
    urlRoot: function() {
        return this.resourceURL('JarApps', {
            fileName: this.get('fileName'),
        });
    },

    idAttribute: 'name',
    
    defaults: {
        'name': '',
        'fileName': '',
        'logicalPlan': {}
    },
    
    initialize: function(attrs, options) {
        
        BaseModel.prototype.initialize.call(this, attrs, options);
        
        if (this.collection && this.collection.fileName) {
            this.set('fileName', this.collection.fileName);
        }
        
        this.on('request', function() {
            this._fetching = true;
        });
        this.on('sync error', function() {
            this._fetching = false;
        });
    },
    
    launch: function(setProperties) {
        
        // Check if setProperties is bool (which means a popup to specify properties should show up)
        if (setProperties === true) {

            // create new pop-up view
            var popup = new LaunchJarWithPropsView({
                model: this
            });

            // append to body
            $('body').append(popup.render().el);

            // launch the modal
            popup.launch();

            // return
            return;
        }

        // Ensure some data
        var data = setProperties || {};
        
        // Get action url
        var url = this.resourceAction('launchApp', {
            fileName: this.get('fileName'),
            appName: encodeURIComponent(this.get('name'))
        });
        
        // Pending message
        Notifier.info({
            title: DT.text('Launching Application...'),
            text: DT.text('This may take a few seconds')
        });
        
        var options = {
            url: url,
            type: 'POST',
            success: function(res) {
                Notifier.success({
                    'title': DT.text('Application Launched'),
                    'text' : DT.text('View the application instance: ') + templates.app_instance_link({
                        appId: res.appId
                    })
                });
            },
            error: function() {
                Notifier.error({
                    'title': DT.text('Error Launching Application'),
                    'text' : DT.text('Ensure that the ' + this.fileName + ' file is still on the Gateway. If it is, check the logs on the DataTorrent Gateway.')
                });
            }
        };

        if (data) {
            options.contentType = 'application/json; charset=utf-8';
            options.data = JSON.stringify(data);
        }

        $.ajax(options);
    },
    
    loadLogicalPlan: function(options) {
        options = options || {};
        if (!_.isEmpty(this.get('logicalPlan'))) {
            
            var plan = new LogicalPlan(this.get('logicalPlan'));
            if (typeof options.success === 'function') {
                options.success(plan);
            }
            
        } else if (this._fetching) {
            this.once('sync', this.loadLogicalPlan.bind(this, options));
            
        } else {
            this.fetch({
                success: _.bind(function() {
                    this.loadLogicalPlan(options);
                }, this)
            });
        }
    }
    
});

exports = module.exports = JarAppModel;