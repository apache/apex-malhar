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
var BasePage = require('bassview');
var Bbind = DT.lib.Bbindings;
var LoginPageView = BasePage.extend({

    initialize: function(options) {
        this.nav = options.app.nav;
        this.model = options.app.user;
        this.subview('userName', new Bbind.text({
            model: this.model,
            attr: 'userName',
            clearErrorOnFocus: true,
            listenToModel: false,
            setAnyway: true,
            classElement: function($el) {
                return $el.parent().parent();
            },
            errorEl: '.help-block',
            errorClass: 'error',
            updateEvents: ['blur', 'update']
        }));
        this.subview('password', new Bbind.text({
            model: this.model,
            attr: 'password',
            clearErrorOnFocus: true,
            listenToModel: false,
            setAnyway: true,
            classElement: function($el) {
                return $el.parent().parent();
            },
            errorEl: '.help-block',
            errorClass: 'error',
            updateEvents: ['blur', 'update']
        }));
    },

    render: function() {
        var json = {
            user: this.model.toJSON(),
            error: this.error
        };
        var html = this.template(json);
        this.$el.html(html);
        this.assign({
            '#userName': 'userName',
            '#password': 'password'
        })
        return this;
    },

    events: {
        'submit #login-form': 'onSubmit'
    },

    onSubmit: function(e) {
        e.preventDefault();
        if (this.model.isValid()) {
            var self = this;
            var promise = this.model.login();
            promise
                .fail(function(xhr, errorThrown, responseText) {
                    self.error = 'An error occurred while trying to log in. Ensure that you entered the correct credentials. (' + xhr.status + ' ' + responseText + ')';
                    self.render();
                })
                .done(function() {
                    self.nav.redirectAuthenticatedUser();
                });
        } else {
            this.subview('userName').$el.trigger('update');
            this.subview('password').$el.trigger('update');
        }
    },

    template: kt.make(__dirname+'/LoginPageView.html')

});
exports = module.exports = LoginPageView;