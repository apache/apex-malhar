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
var LicenseRequestModel = Backbone.Model.extend({

    idAttribute: 'name',

    defaults: {
        name: '',
        company: '',
        country: 'US',
        email: '',
        phone: '',
        type: 'trial'
    },

    defaultsTest: {
        name: 'John Smith',
        company: 'Company, Inc.',
        country: 'US',
        email: 'test@test.com',
        phone: '4151234567',
        type: 'trial'
    },

    validate: function(attrs) {
        // map to hold invalid messages
        var invalid = {};

        if (attrs['name'].trim().length === 0) {
            invalid['name'] = 'Name is required';
        }

        if (attrs['company'].trim().length === 0) {
            invalid['company'] = 'Company is required';
        }

        if (attrs['country'].length === 0) {
            invalid['country'] = 'Country is required';
        }

        if (attrs['email'].trim().length === 0) {
            invalid['email'] = 'Email is required';
        } else if (!this.validateEmail(attrs['email'])) {
            invalid['email'] = 'Email is invalid';
        }

        if (! _.isEmpty(invalid) ) {
            return invalid;
        }
    },

    validateEmail: function (email) {
        var re = /^(([^<>()[\]\\.,;:\s@\"]+(\.[^<>()[\]\\.,;:\s@\"]+)*)|(\".+\"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
        return re.test(email);
    }
});
exports = module.exports = LicenseRequestModel;
