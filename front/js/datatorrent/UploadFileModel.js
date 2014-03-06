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
 * File Model
 * 
 * Models an uploadable file.
*/

var BaseModel = require('./BaseModel');

// class definition
var UploadFileModel = BaseModel.extend({
    
    debugName: 'file',

    uploadMethod: 'PUT',

    // must be specified in child class or options
    putResourceString: '', 
    
    defaults: {
        'name': '',
        'size': 0,
        'type': ''
    },
    
    idAttribute: 'name',
    
    initialize: function(attrs, options) {
        options = options || {};
        BaseModel.prototype.initialize.call(this, attrs, options);
        if (typeof options.beforeUpload === 'function') {
            this.beforeUpload = options.beforeUpload;
        }
        if (options.putResourceString) {
            this.putResourceString = options.putResourceString;
        }
    },
    
    // Uploads this file, requires the formData object
    upload: function() {
        
        var reader = new FileReader();
        var xhr = new XMLHttpRequest();
        var file = this.get('file');
        var self = this;
        
        if (typeof this.beforeUpload === 'function') {
            if ( this.beforeUpload(this) === false ) return false;
        }

        if (this.collection && typeof this.collection.beforeUpload === 'function') {
            if ( this.collection.beforeUpload(this) === false ) return false;
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
        }, false);

        xhr.addEventListener('readystatechange', function() {
            if (this.readyState == 4)  {
                if (this.status != 200 && this.status != 301 && this.status != 302) {
                    self.trigger('upload_error', this.status, this.statusText);
                } else {
                    self.trigger('upload_success', self);
                }
            }
        });
        
        // open the connection
        var url = this.resourceURL(this.putResourceString);
        if (this.uploadMethod === 'PUT') {
            url += '/' + self.get('name'); 
        }
        xhr.open(this.uploadMethod, url);
        
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

exports = module.exports = UploadFileModel;