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

/**
 * SingleFileUploadView
 * 
 * View that contains controls for one file to be uploaded.
 */
var SingleFileUploadView = BaseView.extend({

    className: 'file-to-upload',

    initialize: function(options) {
        this.uploaded = options.uploaded;
        this.listenTo(this.model, 'upload_start', function() {
            this.$('div.progress').show();
        });
        this.listenTo(this.model, 'upload_progress', function(percentage) {
            this.$('.file_upload_progress.bar').css('width', percentage + '%');
        });
        this.listenTo(this.model, 'upload_success', function() {
            this.$('.file_upload_progress.bar').css('width', '100%');
            this.$('div.progress').removeClass('active').removeClass('progress-striped');
        });
    },

    render: function() {

        var json, html;

        json = this.model.toJSON();

        if (this.uploaded && this.uploaded.get(json.name)) {
            json.overwriting = true;
        } else {
            json.overwriting = false;
        }

        html = this.template(json);

        this.$el.html(html);

        this.$('.overwrite-warning').tooltip();

        return this;
    },

    events: {
        'click .remove-file': 'removeFile',
        'click .rename-file': 'renameFile',
        'dblclick .file-upload-filename': 'renameFile',
        'blur input.file-upload-filename': 'saveNewName',
        'keydown input.file-upload-filename': 'checkForEnter'
    },

    removeFile: function() {
        this.collection.remove(this.model);
        this.remove();
    },

    renameFile: function() {
        var $input = $('<input type="text" class="file-upload-filename" value="' + this.model.get('name') + '" />');
        this.$('.file-upload-filename').replaceWith($input);
        $input.focus();
    },

    saveNewName: function() {
        var newName = $.trim(this.$('.file-upload-filename').val());
        
        // check for empty
        if (newName === '') {
            Notifier.error({
                'title': DT.text('Provide a new name'),
                'text': DT.text('The name of the file cannot be an empty string.')
            });
            return;
        }

        if (typeof this.nameFilter === 'function') {
            newName = this.nameFilter(newName);
            if (!newName) {
                return;       
            }
        }

        if (this.model.get('name') === newName) {
            this.render();
            return;
        }

        // check for a file to be uploaded with the same name
        if (this.collection.get(newName)) {
            Notifier.error({
                'title': DT.text('File name taken'),
                'text': DT.text('The name you entered is the name of another file you are already uploading.')
            });
            return; 
        }

        this.model.set('name', newName);

        this.render();
    },

    checkForEnter: function(evt) {
        if (evt.which === 13) {
            this.saveNewName();
        } else if (evt.which === 27) {
            this.render();
        }
    },

    template: kt.make(__dirname+'/SingleFileUploadView.html','_')

});
exports = module.exports = SingleFileUploadView;