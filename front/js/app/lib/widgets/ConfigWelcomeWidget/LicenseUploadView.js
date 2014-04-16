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

var BaseView = require('./StepView');
var UploadLicenseView = require('./UploadLicenseView');
var LicenseFileCollection = require('./LicenseFileCollection');
var Notifier = DT.lib.Notifier;

var LicenseUploadView = BaseView.extend({

    events: {
        'click .continue': 'continue',
        'click .go-to-register': 'goToRegister'
    },

    initialize: function(options) {
        BaseView.prototype.initialize.apply(this, arguments);
        this.dataSource = options.dataSource;
        this.navFlow = options.navFlow;
        this.error = false;
        this.license = options.license;

        var stateOptions = options.stateOptions;
        this.prevStateId = (stateOptions && stateOptions.prevStateId) ? stateOptions.prevStateId : 'WelcomeView';

        // Set a collection for the jar(s) to be uploaded
        this.filesToUpload = new LicenseFileCollection([], {
        });

        this.subview('file-upload', new UploadLicenseView({
            collection: this.filesToUpload
        }));

        this.listenTo(this.filesToUpload, 'upload_success', function() {
            this.navFlow.go('LicenseInfoView', {
                message: 'License file has been successfully uploaded.'
            });
        });

        this.listenTo(this.filesToUpload, 'upload_error', function (jqXHR) {
            this.error = true;
            this.render();
        });
    },

    continue: function (event) {
        event.preventDefault();
    },

    render: function() {
        var html = this.template({
            error: this.error,
            prevStateId: this.prevStateId
        });
        this.$el.html(html);
        if (this.assignments) {
            this.assign(this.assignments);
        }
        return this;
    },

    goToRegister: function (event) {
        event.preventDefault();

        this.navFlow.go('LicenseRegisterView', {
            prevStateId: 'LicenseInfoView'
        });
    },

    assignments: {
        '.file-upload-target': 'file-upload'
    }

});
exports = module.exports = LicenseUploadView;