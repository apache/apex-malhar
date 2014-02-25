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
var BaseView = DT.lib.UploadFilesView;
var SingleJarUploadView = require('./SingleJarUploadView');
var Notifier = DT.lib.Notifier;

/**
 * UploadJarsView
 * 
 * View in JarListWidget for uploading jar(s).
 *
*/
var UploadJarsView = BaseView.extend({

    accept: ['.jar'],

    multiple: true,

    uploadBtnConfirm: 'upload',

    uploadBtnCancel: 'cancel',

    uploadTitle: 'drag and drop jar(s) here',

    uploadText: '(or click to choose)',

    FileView: SingleJarUploadView,

    fileChangeCheck: function(file) {
        var isJar = (file.type === 'application/java-archive');
        if (!isJar) {
            Notifier.error({
                'title': DT.text('Only <code>.jar</code> files accepted'),
                'text': DT.text('incompatible file: ' + file.name)
            });
        }
        return isJar;
    }

});

exports = module.exports = UploadJarsView;
