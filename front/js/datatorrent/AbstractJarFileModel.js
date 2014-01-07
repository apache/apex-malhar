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
 * Jar File Model
 * 
 * Models a jar file that should contain application plans.
*/

var BaseModel = require('./BaseModel');

// abstract class definition
var AbstractJarFileModel = BaseModel.extend({
    
    debugName: 'jarfile',

    putResourceString: 'Jar',
    
    defaults: {
        'name': '',
        'size': 0,
        'type': '',
        'depJar': false
    },
    
    idAttribute: 'name',
    
    initialize: function(attrs, options) {
        options = options || {};
        BaseModel.prototype.initialize.call(this, attrs, options);
        if (typeof options.beforeUpload === 'function') {
            this.beforeUpload = options.beforeUpload;
        }
    },
    
    // Uploads this jar, requires the formData object
    upload: function() {
        
        var reader = new FileReader();
        var xhr = new XMLHttpRequest();
        var file = this.get('file');
        var self = this;
        
        if (this.beforeUpload && typeof this.beforeUpload === 'function') {
            if ( this.beforeUpload(this) === false ) return false;
        }

        this.trigger('upload_start');

        // xhr listeners
        // progress
        xhr.upload.addEventListener('progress', function(e) {
            if (e.lengthComputable) {
                var percentage = Math.round((e.loaded * 100) / e.total);
                self.trigger('upload_progress', percentage);
            }
        }, false);
        
        // complete
        xhr.upload.addEventListener('load', function(e){
            self.trigger('upload_progress', 100);
            self.trigger('upload_success', self);
        }, false);
        
        // open the connection
        xhr.open('PUT', this.resourceURL(this.putResourceString) + '/' + self.get('name'));
        
        // override the mime type of the request
        xhr.overrideMimeType('text/plain; charset=x-user-defined-binary');
        
        // send xhr when the FileReader has completed
        reader.onload = function(evt) {
            xhr.sendAsBinary(evt.target.result);
        };
        
        // start reading the file
        reader.readAsBinaryString(file);
    }
});

exports = module.exports = AbstractJarFileModel;