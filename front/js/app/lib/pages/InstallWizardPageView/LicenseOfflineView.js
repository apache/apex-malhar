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
var BaseView = require('./StepView');
var GatewayInfoModel = require('../../../../datatorrent/GatewayInfoModel');

var LicenseOfflineView = BaseView.extend({

    initialize: function(options) {
        BaseView.prototype.initialize.apply(this, arguments);
        this.loading = true;
        var stateOptions = options.stateOptions;
        this.prevStateId = (stateOptions && stateOptions.prevStateId) ? stateOptions.prevStateId : 'WelcomeView';

        var licenseRequestBlob = options.stateOptions ? options.stateOptions.licenseRequestBlob : '';
        var template = kt.make(__dirname + '/LicenseOfflineRequest.html', '_');

        var about = new GatewayInfoModel({});
        var ajax = about.fetch();

        ajax.done(function () {
            this.loading = false;
            this.requestText = template({
                licenseRequestBlob: licenseRequestBlob,
                hostname: about.get('hostname')
            });
            this.mailtoText = encodeURIComponent(this.requestText);
            this.render();
        }.bind(this));

        ajax.fail(function () { //TODO disable pnotify error
            this.loading = false;
            this.errorMsg = 'Failed to load Gateway Info';
            this.render();
        }.bind(this));
    },

    render: function() {
        var html = this.template({
            loading: this.loading,
            errorMsg: this.errorMsg,
            prevStateId: this.prevStateId,
            mailtoText: this.mailtoText,
            requestText: this.requestText
        });
        this.$el.html(html);

        _.defer(function () {
            this.$el.find('textarea').focus();
            this.$el.find('textarea').select();
        }.bind(this));

        return this;
    }

});

exports = module.exports = LicenseOfflineView;