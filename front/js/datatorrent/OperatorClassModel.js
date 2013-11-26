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
 * Operator Class Model
 * 
 * Models a java class.
*/

var _ = require('underscore'), BaseModel = require('./BaseModel');
var path = require('path');
var settings = require('./settings');
var OperatorClassModel = BaseModel.extend({
    
    defaults: {
        // fully-qualified class name
        name: '',
        // list of the properties that can be set on
        // this class.
        properties: [],
        // actual properties of this class set on the front-end
        propertyValues: {},
        // the input and output ports for this class
        inputPorts: [],
        outputPorts: []
    },
    
    initialize: function(attrs, options) {
        this.dataSource = options.dataSource;
    },
    
    load: function() {
        // Grab the java class name
        var name = this.get('name');
        
        // Clear fields
        if (!name) {
            this.set({
                name: '',
                properties: [],
                propertyValues: {},
                inputPorts: [],
                outputPorts: []
            });
            return;
        }
        
        // Check that dataSource is there
        if (!this.dataSource) {
            throw new Error('OperatorClassModel requires the dataSource to load!');
        }
        
        // Daemon API call
        this.dataSource.getOperatorClass({
            appId: this.get('appId'),
            className: name,
            success: _.bind(function(res) {
                this.set(res);
            }, this)
        });
        
    }
    
});

exports = module.exports = OperatorClassModel;