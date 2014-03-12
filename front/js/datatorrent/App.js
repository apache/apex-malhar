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
/**
 * App Module
 * 
 * This module encapsulates the entire application.
 * It contains the header, nav, and content elements. 
 * 
*/
var _ = require('underscore');
var kt = require('knights-templar');

// Check for REPORTER
if (!window.LOG) {
    window.LOG = function() {
        // do nothing
    };
}

// Polyfills
require('../vendor/sendAsBinary.polyfill');

// Required Jquery plugins
require('../vendor/bootstrap/bootstrap-transition');
require('../vendor/bootstrap/bootstrap-dropdown');
require('../vendor/bootstrap/bootstrap-modal');
require('../vendor/bootstrap/bootstrap-affix');
require('../vendor/pnotify/jquery.pnotify.min');
require('../vendor/jsplumb/jquery.jsPlumb-1.5.2-min.js');
require('../vendor/jquery-ui/ui/minified/jquery-ui.min.js');

// Models
var DataSource   = require('./DataSource');
var NavModel     = require('./NavModel');
var UserModel    = require('./UserModel');
var LicenseModel = require('./LicenseModel');

// Views
var BaseView = require('bassview');
var PageLoaderView = require('./PageLoaderView');
var HeaderView = require('./HeaderView');

// Settings
var settings = require('./settings');

// App Class Definition
var App = BaseView.extend({
    
    initialize: function(options) {
        
        // Get the pages from options
        var pages = options.pages;
        var modes = options.modes;
        
        // Holds the state of the user
        this.user = new UserModel();

        // Has licensing information
        this.license = new LicenseModel({});
        this.license.fetch();

        // Create dataSource instance.
        this.dataSource = new DataSource(options.host, this.user);
        this.dataSource.connect();
        this.listenTo(this.dataSource, 'error', function(res) {
            LOG(4, 'DataSource triggered an error: ', [res]);
        });
        
        // Create the Navigation model
        this.nav = new NavModel({},{
            pages: pages,
            modes: modes
        });
        
        // Top bar with logo, mode switch, and 
        // user sign in and preferences
        this.header = new HeaderView({
            dataSource: this.dataSource,
            model: this.nav,
            user: this.user,
            license: this.license
        });
        
        // This module is responsible for changing page modules
        this.loader = new PageLoaderView({
            pages: pages,     // Pass the page definitions
            app: this,        // Pass the app itself
            model: this.nav   // The loader listens to the nav object
        });
        
        // Render initial markup
        this.render();
        
        // Start history and pickup initial url
        this.nav.start();
        
    },
    
    render: function() {
        this.$el.html(this.template({}));
        this.assign({
            '#header':  this.header,
            '#main':    this.loader
        });
    },
    
    template: kt.make(__dirname+'/App.html', '_')
    
});
exports = module.exports = App;