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
 * Base Collection for App sub collections
 *
 * Acts as base for sub collection that get attached to applications
*/
var _ = require('underscore');
var BaseCollection = require('./BaseCollection');
var ApplicationSubCollection = BaseCollection.extend({
    
    initialize: function(models, options) {
        // Store the dataSource
        this.dataSource = options.dataSource;
        // Store the appId
        this.appId = options.appId;
    },
    
    unsubscribe: function() {
        if (this.dataSource) {
            this.stopListening(this.dataSource);
        }
    }
    
});
exports = module.exports = ApplicationSubCollection;