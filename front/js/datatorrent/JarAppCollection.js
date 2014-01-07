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
 * Jar Application Collection
 * 
 * Represents a collection of Applications in a jar file
 * 
*/
var JarAppModel = require('./JarAppModel');
var BaseCollection = require('./BaseCollection');
var JarAppCollection = BaseCollection.extend({
    
    debugName: 'jar apps',
    
    url: function() {
        return this.resourceURL('JarApps', {
            fileName: this.fileName
        });
    },
    
    initialize: function(models, options) {
        
        BaseCollection.prototype.initialize.call(this, models, options);
        
        this.fileName = options.fileName;
        
    },
    
    model: JarAppModel,
    
    responseTransform: 'applications'
});
exports = module.exports = JarAppCollection;