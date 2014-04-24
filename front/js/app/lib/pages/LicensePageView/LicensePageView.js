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
var Notifier = DT.lib.Notifier;
var kt = require('knights-templar');
var BaseView = require('bassview');

var LicenseFileCollection = require('../../widgets/ConfigWelcomeWidget/LicenseFileCollection');
var UploadLicenseView = require('../../widgets/ConfigWelcomeWidget/UploadLicenseView');

var LicensePageView = BaseView.extend({

    initialize: function(options) {

        // Set a collection for the jar(s) to be uploaded
        this.filesToUpload = new LicenseFileCollection([], {
        });

        this.subview('file-upload', new UploadLicenseView({
            collection: this.filesToUpload
        }));

        this.listenTo(this.filesToUpload, 'upload_success', _.bind(function() {
            // notify user
            Notifier.success({
                title: 'License File Successfully Uploaded',
                text: 'The information on the license page should be updated. If it does not, wait a few moments for the license agent to restart, then refresh the page.'
            });

            // clear out license id
            this.license.unset('id');

            // queue up a fetch on license
            this.license.fetch({
                agentMaxTries: 10
            });
        },this));

        this.listenTo(this.filesToUpload, 'upload_error', function (status, statusText, xhr) {

            var errtitle = 'Error Uploading License (' + xhr.status + ')';
            var errtext  = 'Ensure it is a valid file and try again. If the problem persists, please contact <a href="mailto:support@datatorrent.com">support@datatorrent.com</a>';
            var response = { message: 'An unknown error occurred'};

            if (xhr.status === 400) {
                try {
                    response = JSON.parse(xhr.response);
                } catch (e) {
                    // no parseable response
                }
            }

            errtext = response.message + '. ' + errtext;

            Notifier.error({
                title: errtitle,
                text: errtext
            });
        });

        this.license = options.app.license;
        this.listenTo(this.license.get('agent'), 'sync', this.render);
    },

    render: function() {
        var json = this.license.toJSON()
        var html = this.template(json);
        this.$el.html(html);
        this.assign('.file-upload-target', 'file-upload');
        return this;
    },

    template: kt.make(__dirname+'/LicensePageView.html')

});

exports = module.exports = LicensePageView;