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
var LicenseModal = DT.lib.LicenseModal;
var LicenseModel = DT.lib.LicenseModel;
var LicenseTextModal = require('./LicenseTextModalView');
var LicenseFileCollection = require('./LicenseFileCollection');
var countries = require('./countries');
var Bbind = DT.lib.Bbindings;
var Notifier = DT.lib.Notifier;

var LicenseInfoView = BaseView.extend({

    events: {
        'click .displayLicenseInfo': 'displayLicenseInfo',
        'click .go-to-upload': 'goToUpload',
        'click .go-to-register': 'goToRegister',
        'click .go-to-offline': 'goToOffline'
    },

    initialize: function(options) {
        BaseView.prototype.initialize.apply(this, arguments);
        this.dataSource = options.dataSource;
        this.navFlow = options.navFlow;
        var stateOptions = options.stateOptions;

        if (stateOptions && stateOptions.message) {
            this.message = stateOptions.message;
        }

        this.error = false;
        this.licenseRequestBlob = null;

        var that = this; //TODO make subview

        this.license = options.license;
        this.license.fetch({
            error: function () {
                that.error = true;
                that.render();
            },
            agentMaxTries: 10
        });
        this.listenTo(this.license, 'sync', function () {
            if (this.navFlow.mockState && this.navFlow.mockState.LicenseInfoView) {
                if (this.navFlow.mockState.LicenseInfoView.defaultLicense) {
                    this.license.set('id', 'default-' + this.license.get('id'));
                }
            }

            this.render();
        }.bind(this));
    },

    showLicenseText: function (e) {
        e.preventDefault();

        if (!this.licenseTextModal) {
            this.licenseTextModal = new LicenseTextModal({ model: this.license });
            this.licenseTextModal.addToDOM();
        }
        this.licenseTextModal.launch();
    },

    displayLicenseInfo: function(e) {
        e.preventDefault();

        if (!this.licenseModal) {
            this.licenseModal = new LicenseModal({ model: this.license });
            this.licenseModal.addToDOM();
        }
        this.licenseModal.launch();
    },

    goToUpload: function (event) {
        event.preventDefault();

        this.navFlow.go('LicenseUploadView', {
            prevStateId: 'LicenseInfoView'
        });
    },

    goToRegister: function (event) {
        event.preventDefault();

        this.navFlow.go('LicenseRegisterView', {
            prevStateId: 'LicenseInfoView'
        });
    },

    goToOffline: function (event) {
        event.preventDefault();

        this.navFlow.go('LicenseOfflineView', {
            prevStateId: 'LicenseInfoView',
            licenseRequestBlob: this.licenseRequestBlob
        });
    },

    render: function() {
        var html = this.template({
            error: this.error,
            message: this.message,
            licenseRequestBlob: this.licenseRequestBlob,
            license: this.license
        });
        this.$el.html(html);
        if (this.assignments) {
            this.assign(this.assignments);
        }
        return this;
    },

    assignments: {
    }

});
exports = module.exports = LicenseInfoView;