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

var _ = require('underscore');
var kt = require('knights-templar');
var Notifier = DT.lib.Notifier;
var BaseView = require('bassview');
var SingleJarUploadView = require('./SingleJarUploadView');

/**
 * UploadJarsView
 * 
 * View in JarListWidget for uploading jar(s).
 *
*/
var UploadJarsView = BaseView.extend({
    
    initialize: function(options) {

        this.uploaded = options.uploaded;
        
        // Listen for upload progress and complete on the model
        this.listenTo(this.collection, 'reset', this.render);
        this.listenTo(this.collection, 'remove', function(model, collection) {
            if (collection.length === 0) {
                this.render();
            }
        });
    },
    
    render: function() {
        
        var html = this.template({
            files: this.collection.toJSON()
        });
        this.$el.html(html);
        var $jars = this.$('.jars-to-upload');
        this.collection.each(function(file) {

            // Create view for individual file
            var view = new SingleJarUploadView({
                model: file,
                collection: this.collection,
                uploaded: this.uploaded
            });

            // Append single view to this view
            $jars.append(view.render().el);

        }, this);

        return this;
    },
    
    events: {
        'change .jar_upload': 'onFileChange',
        'click .jar_upload_target': 'onClick',
        'dragover .jar_upload_target': 'onDragOver',
        'drop .jar_upload_target': 'onDrop',
        'click .cancel_jar_btn': 'clearFiles',
        'click .upload_jar_btn': 'uploadJars',
        'click .jar-to-upload': function(evt) {
            evt.stopPropagation();
        },
        'submit .jar_upload_form': 'preventDefault'
    },
    
    onFileChange: function() {
        
        var files = this.el.querySelector('.jar_upload').files;
        
        var filteredFiles = [];

        for (var i = files.length - 1; i >= 0; i--) {
            var file = files[i];
            if (!/\.jar$/.test(file.name)) {
                Notifier.error({
                    'title': 'Only <code>.jar</code> files accepted',
                    'text': 'incompatible file: ' + file.name
                });
                // Re-render to clear out file
                return this.render();
            }

            // It's a jar!
            filteredFiles.push({
                'name': file.name,
                'size': file.size,
                'type': file.type,
                'file': file
            });
        };

        this.collection.reset(filteredFiles, { remove: true });

    },
    
    onClick: function() {
        
        this.el.querySelector('.jar_upload').click();
        
    },
    
    onDragOver: function(evt) {
        evt.preventDefault();
    },
    
    onDrop: function(evt) {
        evt.preventDefault();
        evt.stopPropagation();

        // Get the file dropped
        var files = evt.originalEvent.dataTransfer.files;
        
        // Set them to the input.files object
        this.el.querySelector('.jar_upload').files = files;
    },
    
    onProgress: function(progress) {
        var bar = this.$('.jar_upload_progress');
        
        bar.css('width', progress + '%');
        
        if (progress === 100) {
            bar.removeClass('progress-striped').removeClass('active');
        }
    },
    
    onSuccess: function(jar) {
        // clear out the model
        this.model.clear();
        // notify success
        Notifier.success({
            'title': 'Successfully uploaded jar',
            'text': 'It should be visible on the list of uploaded jars.'
        });
        
        // remove buttons
        this.$('.upload-buttons').html('');
        this.$('.jar_upload_target .progress').removeClass('progress-striped').removeClass('active');
        
        // render and clear
        setTimeout(_.bind(function() {
            this.render();
        }, this), 2000);
    },
    
    onComplete: function() {
        this.$('.upload_jar_btn').attr('disabled', false);
    },
    
    onError: function(xhr, err, responseText) {
        this.$('.jar_upload_progress').css('width', '0%');
        Notifier.error({
            'title': 'An error occurred uploading jar',
            'text': 'Server responded with: ' + responseText
        });
    },

    uploadJars: function(evt) {
        evt.preventDefault();
        evt.stopPropagation();

        var pending = this.collection.length;

        this.listenTo(this.collection, 'upload_success', function() {
            if (--pending === 0) {
                this.collection.reset([]);
            }
        });

        this.collection.each(function(file) {
            file.upload();
        });
    },
    
    clearFiles: function(evt) {
        evt.stopPropagation();
        evt.preventDefault();
        this.collection.reset([]);
    },

    preventDefault: function(evt) {
        evt.preventDefault();
    },
    
    template: kt.make(__dirname+'/UploadJarsView.html','_')
    
});

exports = module.exports = UploadJarsView;
