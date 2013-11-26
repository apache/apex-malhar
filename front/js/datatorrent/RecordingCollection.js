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
var BaseCollection = require('./BaseCollection');
var RecordingModel = require('./RecordingModel');
var RecordingCollection = BaseCollection.extend({
    
    debugName: 'recordings',
    
    model: RecordingModel,
    
    initialize: function(models,options) {
        this.dataSource = options.dataSource;
        this.appId = options.appId;
        this.operatorId = options.operatorId;
    },
    
    responseTransform: 'recordings',
    
    url: function() {
        if (this.operatorId) {
            // If operator has been specified, use operator-specific REST call.
            return this.resourceURL('Recording', {
                appId: this.appId,
                operatorId: this.operatorId
            });
        } else {
            // Otherwise, just get all the recordings for this application.
            return this.resourceURL('AppRecordings', {
                appId: this.appId
            });
        }
    }
    
});
exports = module.exports = RecordingCollection;