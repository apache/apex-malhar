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
 * PageLoader View
 * 
 * Loads in pages based on url
*/
var _ = require('underscore');
var kt = require('knights-templar');
var BaseView = require('bassview');
var Breadcrumbs = require('../Breadcrumbs').collection;
var breadcrumbsView = require('../Breadcrumbs').colview;
var Notifier = require('../Notifier');
var PageLoaderView = BaseView.extend({
    
    initialize: function(options) {
        
        // Stores view of current page
        this.loaded_page = false;
        
        // Hash lookup of all page modules
        this.pages = options.pages;
        
        // Breadcrumbs for current page
        this.breadcrumbs = new Breadcrumbs([], {
            nav: this.model,
            loader: this
        });
        this.breadcrumbsView = new breadcrumbsView({
            collection: this.breadcrumbs,
            model: this.model
        });
        
        // Listen to the nav for changes
        this.listenTo(this.model, "change:current_page change:url_args", this.changePage );
        
        // Listen to window for resizing
        $(window).on('resize', _.bind(this.emitResize, this));
        
        // Set options as property
        this.options = options;
    },
    
    render: function() {
        if (this.loaded_page === false) return this;

        // Render the template
        var markup = this.template({
            useDashMgr: this.loaded_page.useDashMgr
        });
        this.$el.html(markup);
        
        // Assign views to their elements
        this.assign({
            '#pagecontent': this.loaded_page,
            '#breadcrumbs': this.breadcrumbsView
        });
    },
    
    changePage: function(nav) {
        // Trigger clean up of previous
        this.trigger('clean_up_current');

        var page = this.model.get('current_page');
        
        // Set up initial page module object
        var loaded_page = {}
        var pageMetadata = _.find(this.pages, function(pageDef) { return pageDef.name === page });
        var urlArgs = nav.get('url_args');
        var pageParams = _.object(pageMetadata.paramList, urlArgs);

        // Instantiate Page View
        loaded_page = new pageMetadata.view({
            app: this.options.app,
            pageParams: pageParams
        });

        // Set up bread crumbs
        loaded_page.breadcrumbs = pageMetadata.breadcrumbs;

        // Set listener on page change
        loaded_page.listenTo(this, "clean_up_current", loaded_page.cleanUp);

        // Clear any previously loaded page module
        this.loaded_page = loaded_page;

        // Set the mode from the pages hash
        var mode = nav.modes.get(pageMetadata.mode);
        if (mode) {
            nav.modes.clearActive({silent: true});
            mode.set('active', true);
        } else {
            LOG(3, 'Page did not have valid mode', ['mode: ', pageMetadata.mode]);
        }

        // Set the breadcrumbs
        this.breadcrumbs.reset(loaded_page.breadcrumbs);
        
        // Render when the dashboards have been loaded
        this.render();

    },

    changeUrlArgs: function(nav, url_args) {
        if (this.loaded_page !== false) this.loaded_page.trigger("change:url_args",url_args);
    },
    
    emitResize: _.debounce(function() {
        
        var loadedDash, widgets;
        
        if (!this.loaded_page) return;
        
        loadedDash = this.loaded_page.__wDashes.find(function(dash) {
            return dash.get('selected');
        });
        
        if (loadedDash) {
            loadedDash.triggerResize();
        }
        
    }, 300 ),
    
    template: kt.make(__dirname+'/PageLoaderView.html', '_'),

});
exports = module.exports = PageLoaderView;