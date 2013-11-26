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
 * JarUploadWidget
 * 
 * Widget for uploading jar files
 *
*/

var _ = require('underscore');
var kt = require('knights-templar');
var Notifier = DT.lib.Notifier;
var BaseView = require('bassview');
var JarFileModel = DT.lib.JarFileModel;

// class definition
var JarUploadWidget = BaseView.extend({
    
    initialize: function(options) {
        
        // Listen for upload progress and complete on the model
        this.listenTo(this.model, 'upload_progress', this.onProgress);
        this.listenTo(this.model, 'upload_success', this.onSuccess);
        this.listenTo(this.model, 'upload_complete', this.onComplete);
        this.listenTo(this.model, 'upload_error', this.onError);
    },
    
    render: function() {
        
        var html = this.template(this.model.toJSON());
        this.$el.html(html);
        return this;
    },
    
    events: {
        'change .jar_upload': 'onFileChange',
        'click .jar_upload_target': 'onClick',
        'dragover .jar_upload_target': 'onDragOver',
        'drop .jar_upload_target': 'onDrop',
        'click .cancel_jar_btn': 'clearFile',
        'click .upload_jar_btn': 'uploadJar'
    },
    
    onFileChange: function() {
        
        var files = this.el.querySelector('.jar_upload').files;
        var file = files[0];
        
        if (!/\.jar$/.test(file.name)) {
            Notifier.error({
                'title': 'Only <code>.jar</code> files accepted',
                'text': 'Ensure that you have dropped or selected a java archive file (.jar).'
            });
            // Re-render to clear out file
            return this.render();
        }

        // It's a jar!
        this.model.set({
            'name': file.name,
            'size': file.size,
            'type': file.type,
            'file': file
        });
        this.render();
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

    uploadJar: function(evt) {
        evt.preventDefault();
        evt.stopPropagation();
        
        var formData = new FormData(this.$('.jar_upload_form')[0]);
        
        var result = this.model.upload(formData);
        
        // disable the button
        if (result !== false) {
            $(evt.target).attr('disabled', true);
        }
    },
    
    clearFile: function(evt) {
        evt.stopPropagation();
        evt.preventDefault();
        this.model.clear();
        this.render();
    },
    
    template: kt.make(__dirname+'/JarUploadView.html','_')
    
});

exports = module.exports = JarUploadWidget;
