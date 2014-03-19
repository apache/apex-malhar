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

var settings = require('../settings');

function GatewayPoll(timeout) {
    this.timeout = timeout;
    this.savedId = null;
    this.aboutUrl = settings.interpolateParams(settings.urls.GatewayInfo, {
        v: settings.version
    });
}

GatewayPoll.prototype.restartRequest = function () {
    var url = settings.interpolateParams(settings.actions.restartGateway, {
        v: settings.version
    });

    return $.post(url);
};

GatewayPoll.prototype.initId = function () {
    // get initial jvmName
    var ajax = $.get(this.aboutUrl);

    ajax.done(function (data) {
        this.savedId = data.jvmName;
    }.bind(this));

    return ajax;
};

GatewayPoll.prototype.start = function () {
    this.deferred = $.Deferred();

    this.startTime = Date.now();
    this.poll();

    return this.deferred.promise();
};

GatewayPoll.prototype.poll = function () {
    var ajax = $.get(this.aboutUrl);

    ajax.done(function (data) {
        if (this.savedId !== data.jvmName) {
            this.deferred.resolve();
        }
    }.bind(this));

    ajax.always(function () {
        var state = this.deferred.state();
        if (state === 'pending') {
            if (Date.now() - this.startTime > this.timeout) {
                this.deferred.reject();
            } else {
                setTimeout(this.poll.bind(this), 500);
            }
        }
    }.bind(this));
};


exports = module.exports = GatewayPoll;