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

    title: 'Restart the Gateway',

    closeBtn: false,

    initialize: function(options) {
        BaseView.prototype.initialize.call(this, {
            launchOptions: {
                backdrop: 'static'
            }
        });

        this.dataSource = options.dataSource;
        this.message = options.message;
        this.restartCompleteCallback = options.restartCompleteCallback;
        this.restarting = false;
        this.poll = new GatewayPoll(10000);

        if (options.prompt !== true) {
            this.doRestart();
        }
    },

    // Initiates gateway restart
    doRestart: function(e) {
        if (e && typeof e.preventDefault === 'function') {
            e.preventDefault();
        }
        this.restarting = true;
        this.confirmText = this.cancelText = false;
        this.render();

        // Start restart process
        var initPromise = this.poll.initId(); // get current jvmName with PID

        // on initId resolve
        initPromise.done(function () {
            // trigger restart
            this.dataSource.disconnect();
            var restartRequestPromise = this.poll.restartRequest();

            // on restart resolve
            restartRequestPromise.done(function () {
                
                var promise = this.poll.start(); // poll for jvmName change
                promise.done( this.restartSucceeded.bind(this) );
                promise.fail( this.restartFailed.bind(this) );

            }.bind(this));

            // on restart reject
            restartRequestPromise.fail( this.restartFailed.bind(this) );

        }.bind(this));

        // on initId reject
        initPromise.fail( this.restartFailed.bind(this) );
    },

    restartSucceeded: function() {
        Notifier.success({
            title: 'Restart Successful',
            text: 'Gateway has been successfully restarted.'
        });
        this.dataSource.connect();
        if (this.restartCompleteCallback) {
            this.restartCompleteCallback.call();
        }
        this.close();
        this.reset();
    },

    restartFailed: function () {
        Notifier.error({
            title: 'Restart Failed',
            text: 'Please issue the following to your command line terminal to force the DT Gateway to start: ' +
                  '<br/><span style="font-family:Consolas,Courier,monospace;">service dtgateway start</span>',
            hide: false
        });
        this.close();
        this.reset();
    },

    reset: function() {
        this.restarting = false;
        this.cancelText = RestartModalView.prototype.cancelText;
        this.confirmText = RestartModalView.prototype.confirmText;
        this.render();
    },

    body: function() {
        var json = {
            message: this.message,
            restarting: this.restarting,
            confirm_message: text('Are you sure you want to restart the gateway?')
        };
        var html = this.template(json);
        return html;
    },

    events: {
        'click .cancelBtn': 'close',
        'click .confirmBtn': 'doRestart'
    },

    cancelText: text('no'),

    confirmText: text('yes'),

    template: kt.make(__dirname + '/RestartModalView.html', '_')

});
exports = module.exports = RestartModalView;