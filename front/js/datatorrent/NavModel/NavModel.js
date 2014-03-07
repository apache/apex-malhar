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
var util = require('./util');
var Modes = require('../ModeCollection');
var Nav = Backbone.Model.extend({

    defaults: {
        current_page: "",
        url_args: [],
        mode: ""
    },

    initialize: function(attributes, options) {
        // Create a router
        this.router = new Backbone.Router({
            routes: util.extractRoutesFrom(options.pages)
        });

        this.modes = new Modes(options.modes);

        // Listen for routes
        this.listenTo(this.router, "route", this.onRouteChange);
    },

    onRouteChange: function(route, params) {
        var args = params.slice();

        // If the page is changing, dont trigger the url change
        var silence_args = (route != this.get('current_page'));

        // Make changes
        this.set({
            'url_args': args
        }, {
            silent: silence_args
        });
        this.set('current_page', route);
    },

    start: function() {
        Backbone.history.start();
    },

    go: function(route, options) {
        this.router.navigate(route, options);
    }

});

exports = module.exports = Nav