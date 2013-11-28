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
 * Port Collection
 * 
 * Represents a group of ports
*/
var PortModel = require('./PortModel');
var BaseCollection = require('./BaseCollection');
var PortCollection = BaseCollection.extend({
    
    debugName: 'ports',
    
    model: PortModel,
    
    initialize: function(models, options) {
        this.dataSource = options.dataSource;
        this.appId = options.appId;
        this.operatorId = options.operatorId;
    },
    
    url: function() {
        return this.resourceURL('Port', {
            appId: this.appId,
            operatorId: this.operatorId
        });
    }
    
});
exports = module.exports = PortCollection;