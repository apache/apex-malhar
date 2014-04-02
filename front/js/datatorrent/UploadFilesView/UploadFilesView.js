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
var Notifier = require('../Notifier');
var BaseView = require('bassview');
var SingleFileUploadView = require('./SingleFileUploadView');


/**
 * UploadFilesView
 * 
 * View in FileListWidget for uploading file(s).
 *
 * Usage:
 *
 *     var view = new UploadFilesView({
 *         collection: new DT.lib.UploadFileCollection([])
 *     });
 *     
 *     $('body').append(view.render().el);
 *
 *
*/
var UploadFilesView = BaseView.extend({
    
    /**
     * @param  {DT.lib.UploadFileCollection} options.collection  Holds files to be uploaded, and clears on upload success.
     * @param  {DT.lib.UploadFileCollection} options.uploaded    (optional) Reference to files on server to check against for overwrites.
     */
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

    FileView: SingleFileUploadView,

    // file type restrictions
    // eg. ['.jar','.zip']
    accept: [],

    multiple: true,

    uploadBtnConfirm: 'upload',

    uploadBtnCancel: 'cancel',

    uploadTitle: 'drag and drop files here',

    uploadText: '(click to choose files)',
    
    events: {
        'change .file_upload': 'onFileChange',
        'click .file_upload_target': 'onClick',
        'dragover .file_upload_target': 'onDragOver',
        'drop .file_upload_target': 'onDrop',
        'click .cancel_file_btn': 'clearFiles',
        'click .upload_file_btn': 'uploadFiles',
        'click .file-to-upload': function(evt) {
            evt.stopPropagation();
        },
        'submit .file_upload_form': 'preventDefault'
    },

    render: function() {
        
        var html = this.template({
            files: this.collection.toJSON(),
            accept: this.accept.join(','),
            multiple: _.result(this, 'multiple'),
            uploadTitle: _.result(this, 'uploadTitle'),
            uploadText: _.result(this, 'uploadText'),
            uploadBtnConfirm: _.result(this, 'uploadBtnConfirm'),
            uploadBtnCancel: _.result(this, 'uploadBtnCancel')
        });
        this.$el.html(html);
        var $files = this.$('.files-to-upload');
        this.collection.each(function(file) {

            // Create view for individual file
            var view = new this.FileView({
                model: file,
                collection: this.collection,
                uploaded: this.uploaded
            });

            // Append single view to this view
            $files.append(view.render().el);

        }, this);

        return this;
    },
    
    onFileChange: function() {
        
        var files = this.el.querySelector('.file_upload').files;
        
        var filteredFiles = [];

        var hasFileChangeCheck = typeof this.fileChangeCheck === 'function';

        for (var i = files.length - 1; i >= 0; i--) {
            
            var file = files[i];

            if (hasFileChangeCheck) {
                var passed = this.fileChangeCheck(file);
                if (!passed) {
                    this.render();
                    return;
                }
            }

            filteredFiles.push({
                'name': file.name,
                'size': file.size,
                'type': file.type,
                'file': file
            });
        }

        this.collection.reset(filteredFiles, { remove: true });
    },
    
    onClick: function() {
        
        this.el.querySelector('.file_upload').click();
        
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
        this.el.querySelector('.file_upload').files = files;
    },
    
    onProgress: function(progress) {
        var bar = this.$('.file_upload_progress');
        
        bar.css('width', progress + '%');
        
        if (progress === 100) {
            bar.removeClass('progress-striped').removeClass('active');
        }
    },
    
    onSuccess: function(file) {
        // clear out the model
        this.model.clear();
        // notify success
        Notifier.success({
            'title': 'Successfully uploaded file',
            'text': 'It should be visible on the list of uploaded file.'
        });
        
        // remove buttons
        this.$('.upload-buttons').html('');
        this.$('.file_upload_target .progress').removeClass('progress-striped').removeClass('active');
        
        // render and clear
        setTimeout(_.bind(function() {
            this.render();
        }, this), 2000);
    },
    
    onComplete: function() {
        this.$('.upload_file_btn').attr('disabled', false);
    },
    
    onError: function(xhr, err, responseText) {
        this.$('.file_upload_progress').css('width', '0%');
        Notifier.error({
            'title': 'An error occurred uploading file',
            'text': 'Server responded with: ' + responseText
        });
    },

    uploadFiles: function(evt) {
        evt.preventDefault();
        evt.stopPropagation();

        var pending = this.collection.length;

        this.listenTo(this.collection, 'upload_success upload_error', function() {
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
    
    template: kt.make(__dirname+'/UploadFilesView.html','_')
    
});

exports = module.exports = UploadFilesView;
