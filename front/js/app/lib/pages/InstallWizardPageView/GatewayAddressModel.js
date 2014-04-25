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
var Backbone = require('backbone');
var GatewayAddressModel = Backbone.Model.extend({

    defaults: {
        ip: '',
        port: ''
    },

    initialIP: '',
    initialPort: '',

    init: function (model) {
        this.initialIP = model.get('ip');
        this.initialPort = model.get('port');

        this.set('ip', this.initialIP);
        this.set('port', this.initialPort);
    },

    getValue: function () {
        return this.get('ip') + ':' + this.get('port');
    },

    isChanged: function () {
        return (this.get('ip') !== this.initialIP) || (this.get('port') != this.initialPort);
    },

    validate: function(attrs) {
        // map to hold invalid messages
        var invalid = {};

        if (attrs['ip'].trim().length === 0) {
            invalid['ip'] = 'IP Address is required';
        } else if (!this.validateIP(attrs['ip'])) {
            invalid['ip'] = 'IP Address is invalid';
        }

        if (attrs['port'].trim().length === 0) {
            invalid['port'] = 'Port is required';
        } else if (!this.validatePort(attrs['port'])) {
            invalid['port'] = 'Port is invalid';
        }

        if (! _.isEmpty(invalid)) {
            return invalid;
        }
    },

    validateIP: function (ip) {
        var regExp = /^(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)$/;
        return regExp.test(ip);
    },

    validatePort: function (port) {
        var regExp = /^\d+$/;
        return regExp.test(port);
    }
});
exports = module.exports = GatewayAddressModel;
