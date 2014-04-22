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
var _ = require('underscore'), Backbone = require('backbone');
var BaseModel = require('../BaseModel');
var BaseUtil = require('../BaseUtil');
var text = require('../text');
var User = BaseModel.extend({
    
    debugName: 'User',

    defaults: {
        authenticated: false,
        authEnabled: false
    },

    url: function() {
        return this.resourceURL('User');
    },

    fetchError: BaseUtil.quietFetchError,

    defaults: {
        userName: '',
        password: '',
        roles: []
    },

    validate: function(attrs) {
        var errors = {};

        if (!attrs.userName) {
            errors.userName = text('Please enter a username');
        }

        if (!attrs.password) {
            errors.password = text('Please enter a password');
        }

        if (!_.isEmpty(errors)) {
            return errors;
        }
    },

    login: function() {
        var promise = $.ajax({
            url: this.resourceAction('loginUser'),
            type: 'POST',
            contentType: 'application/json; charset=utf-8',
            xhrFields: {
                withCredentials: true
            },
            data: JSON.stringify({
                userName: this.get('userName'),
                password: this.get('password')
            })
        });
        var self = this;
        promise.done(function() {
            self.set('authenticated', true);
        });
        return promise;
    },

    logout: function() {
        var promise = $.ajax({
            url: this.resourceAction('logoutUser'),
            type: 'POST',
            xhrFields: {
                withCredentials: true
            }
        });
        return promise;
    }
    
});

exports = module.exports = User