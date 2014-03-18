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
var text = require('../../../../datatorrent/text');
var BaseView = DT.lib.ModalView;

/**
 * Modal that shows license information
 */
var ConfirmModalView = BaseView.extend({

    title: 'Restart Required',

    initialize: function(options) {
        BaseView.prototype.initialize.call(this, {
            launchOptions: {
                backdrop: 'static'
            }
        });

        this.message = options.message;
        this.confirmCallback = options.confirmCallback;
    },

    body: function() {
        var json = {
            message: this.message
        };
        var html = this.template(json);
        return html;
    },

    events: {
        'click .cancelBtn': 'onCancel',
        'click .confirmBtn': 'onConfirm'
    },

    onConfirm: function(evt) {
        BaseView.prototype.onConfirm.call(this, evt);
        this.confirmCallback.call();
    },

    confirmText: 'restart',

    cancelText: 'restart later',

    template: kt.make(__dirname + '/ConfirmModalView.html', '_')

});
exports = module.exports = ConfirmModalView;