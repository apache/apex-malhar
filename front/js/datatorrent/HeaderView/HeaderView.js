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
var BaseView = require('bassview');

/**
 * Header View
 * 
 * The top bar of the UI.
 * Contains platform/client logo, mode switch
 * (development/operations), sign in
*/
var Header = BaseView.extend({
    
    UIVersion: '',
    
    initialize: function(options) {
        this.listenTo(this.model, "change:mode", this.render );
        options.dataSource.getUIVersion(
            _.bind(function(version){
                this.UIVersion = version;
                this.render();
            },this)
        );
    },
    
    events: {
        'click .displayLicenseInfo': 'displayLicenseInfo',
        'click .displayVersion': 'displayVersion'
    },

    displayVersion: function(e) {
        e.preventDefault();
    },

    displayLicenseInfo: function(e) {
        e.preventDefault();
    },
    
    render: function() {
        var markup = this.template({
            modes: this.model.serializeModes(),
            client_logo: "client_logo_hadoop.jpg",
            version: this.UIVersion
        });
        // this.$('#header .brand').tooltip('destroy');
        this.$el.html(markup);
        this.$el.addClass('navbar-fixed-top').addClass('navbar');
        // this.$('#header .brand').tooltip({
        //     position: { my: "left-10 top+5", at: "middle bottom", collision: "flipfit" }
        // });
        return this;
    },

    template: kt.make(__dirname+'/HeaderView.html', '_')
});
exports = module.exports = Header