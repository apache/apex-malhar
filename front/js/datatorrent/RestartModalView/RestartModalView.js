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
var text = require('../text');
var BaseView = require('../ModalView');
var GatewayPoll = require('./GatewayPoll');
var Notifier = require('../Notifier');

/**
 * Modal that shows license information
 */
var RestartModalView = BaseView.extend({

    title: 'Restarting',

    closeBtn: false,

    initialize: function(options) {
        this.dataSource = options.dataSource;
        this.message = options.message;
        this.restartCompleteCallback = options.restartCompleteCallback;

        var poll = new GatewayPoll(10000);

        var initPromise = poll.initId(); // get current jvnName with PID
        initPromise.done(function () {
            // trigger restart
            this.dataSource.disconnect();
            var restartRequestPromise = poll.restartRequest();
            restartRequestPromise.done(function () {
                var promise = poll.start(); // poll for jvnName change

                promise.done(function () {
                    Notifier.success({
                        title: 'Gateway',
                        text: 'Gateway has been successfully restarted.'
                    });
                    this.dataSource.connect();
                    if (this.restartCompleteCallback) {
                        this.restartCompleteCallback.call();
                    }
                    this.close();
                }.bind(this));

                promise.fail(function () {
                    this.restartFailed();
                }.bind(this));
            }.bind(this));

            restartRequestPromise.fail(function () {
                this.restartFailed();
            }.bind(this));
        }.bind(this));

        initPromise.fail(function () {
            this.restartFailed();
        }.bind(this));
    },

    restartFailed: function () {
        Notifier.error({
            title: 'Failed to restart',
            text: 'Please issue the following to your command line terminal to force the DT Gateway to start: '
                + '<br/><span style="font-family:Consolas,Courier,monospace;">service dtgateway start</span>',
            hide: false
        });

        this.close();
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

    confirmText: text('close'),

    cancelText: false,

    confirmText: false,

    template: kt.make(__dirname + '/RestartModalView.html', '_')

});
exports = module.exports = RestartModalView;