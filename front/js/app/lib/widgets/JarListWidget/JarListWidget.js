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

var Tabled = DT.lib.Tabled;
var _ = require('underscore');
var kt = require('knights-templar');
var columns = require('./columns');
var BaseView = DT.widgets.ListWidget;
var JarFileModel = DT.lib.AppJarFileModel;
var JarFileCollection = DT.lib.AppJarFileCollection;
var Palette = require('./JarListPalette');
var UploadJarsView = require('./UploadJarsView');

/**
 * JarListWidget
 * 
 * This widget displays a list of uploaded jars
 * and also allows users to upload jars themselves.
 *
*/
var JarListWidget = BaseView.extend({

    Collection: JarFileCollection,

    Model: JarFileModel,
    
    initialize: function(options) {
        
        BaseView.prototype.initialize.call(this, options);
        
        // Set a collection for the jar(s) to be uploaded
        this.jarsToUpload = new this.Collection([], {
            beforeUpload: _.bind(function(model) {
                if (this.collection.get(model.get('name'))) {
                    return confirm('This will overwrite a jar on the server, do you want to proceed?');
                }
            }, this)
        });
        
        // Retrieve current jars on server
        this.collection = new this.Collection([]);
        this.collection.fetch();
        
        // When an upload succeeds, updated jars on server.
        this.collection.listenTo(this.jarsToUpload, 'upload_success', _.bind(_.debounce(function() {
            this.collection.fetch();
        }, 1000), this));
        
        // Set up the table
        var columns = require('./columns');
        this.subview('tabled', new Tabled({
            collection:this.collection,
            columns:columns,
            id: 'dev.jarlist.'+this.compId(),
            save_state: true,
            row_sorts: ['name']
        }));
        
        this.subview('uploader', new UploadJarsView({
            collection: this.jarsToUpload,
            uploaded: this.collection
        }));
        
        // Set up the palette
        this.subview("palette", new Palette({
            collection: this.collection,
            nav: options.nav,
            dataSource: this.dataSource
        }));
    },
    
    assignments: function() {
        var assignments = BaseView.prototype.assignments.call(this);
        assignments['.uploader-target'] = 'uploader';
        return assignments;
    },
    
    template: kt.make(__dirname+'/JarListWidget.html','_'),
    
    remove: function() {
        this.collection.stopListening();
        BaseView.prototype.remove.call(this);
    }
    
});

exports = module.exports = JarListWidget;