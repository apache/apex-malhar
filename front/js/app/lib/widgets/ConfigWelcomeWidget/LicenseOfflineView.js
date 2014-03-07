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
var BaseView = require('./StepView');

var LicenseOfflineView = BaseView.extend({

    initialize: function(options) {
        BaseView.prototype.initialize.apply(this, arguments);
        var stateOptions = options.stateOptions;
        this.prevStateId = (stateOptions && stateOptions.prevStateId) ? stateOptions.prevStateId : 'WelcomeView';

        this.requestText = options.stateOptions.licenseRequestBlob;
        this.mailtoText = encodeURIComponent(this.requestText);
    },

    render: function() {
        var html = this.template({
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