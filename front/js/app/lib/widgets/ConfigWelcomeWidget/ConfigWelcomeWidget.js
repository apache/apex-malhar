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
var kt = require('knights-templar');
var WidgetView = DT.lib.WidgetView;
var StepView = require('./StepView');
var WelcomeView = require('./WelcomeView');
var SystemView = require('./SystemView');
var LicenseInfoView = require('./LicenseInfoView');
var LicenseRegisterView = require('./LicenseRegisterView');
var LicenseUploadView = require('./LicenseUploadView');
var LicenseOfflineView = require('./LicenseOfflineView');
var SummaryView = require('./SummaryView');
var HadoopView = require('./HadoopView');

/**
 * ConfigWelcomeWidget
 * 
 * Welcomes the user to the datadorrent console.
 * Takes the form of a "install wizard" with 
 * sequential steps that show potential issues and
 * offer ways of fixing those issues.
*/
var ConfigWelcomeWidget = WidgetView.extend({

    events: {
        'click .install-step-link[data-action]': 'onStepLinkClick'
    },

    initialize: function(options) {
        WidgetView.prototype.initialize.call(this, options);

        this.dataSource = options.dataSource;

        this.license = options.app.license;

        this.activeStateId = 'WelcomeView';
        //this.activeStateId = 'LicenseInfoView';
        //this.activeStateId = 'LicenseRegisterView';
        //this.activeStateId = 'LicenseOfflineView';
        //this.activeStateId = 'LicenseUploadView';
        //this.activeStateId = 'SystemView';
        //this.activeStateId = 'SummaryView';
    },

    render: function() {
        //this.mockState = window.mockState; //for dev testing only

        // Sets up the base markup for the wizard
        var html = this.template({});
        this.$el.html(html);

        // Goes to active step.
        //this.goToStep(this.steps.getActive());
        this.go(this.activeStateId);

        // Allow chaining
        return this;
    },

    onStepLinkClick: function(e) {
        e.preventDefault();
        var $target = $(e.target);

        if (!$target.hasClass('install-step-link')) {
            // look for parent
            $target = $target.parents('.install-step-link');
        }

        var step = $target.data('action');
        if (step) {
            //this.steps.setActive(step);
            this.go(step);
        }
    },
    
    // base markup for the wizard
    template: kt.make(__dirname+'/ConfigWelcomeWidget.html','_'),

    navStates: {
        WelcomeView: {
            view: WelcomeView,
            template: kt.make(__dirname+'/WelcomeView.html'),
            indicator: 'welcome'
        },
        LicenseInfoView: {
            view: LicenseInfoView,
            template: kt.make(__dirname+'/LicenseInfoView.html'),
            indicator: 'license'
        },
        // LicenseRegisterView: {
        //     view: LicenseRegisterView,
        //     template: kt.make(__dirname+'/LicenseRegisterView.html'),
        //     indicator: 'license'
        // },
        LicenseUploadView: {
            view: LicenseUploadView,
            template: kt.make(__dirname+'/LicenseUploadView.html'),
            indicator: 'license'
        },

        // LicenseOfflineView: {
        //     view: LicenseOfflineView,
        //     template: kt.make(__dirname+'/LicenseOfflineView.html'),
        //     indicator: 'license'
        // },
        // SystemView: {
        //     view: SystemView,
        //     template: kt.make(__dirname+'/SystemView.html'),
        //     indicator: 'welcome'
        // },
        HadoopView: {
            view: HadoopView,
            template: kt.make(__dirname+'/HadoopView.html'),
            indicator: 'hadoop'
        },
        SummaryView: {
            view: SummaryView,
            template: kt.make(__dirname+'/SummaryView.html'),
            indicator: 'summary'
        }
    },

    go: function (stateId, stateOptions) {
        var state = this.navStates[stateId];

        if (!state) {
            throw new Error('No view found for "' + stateId + '" state!');
        }

        // Remove old current view if present
        if (this._currentView) {
            this._currentView.remove();
        }

        // Create and render new view.
        this._currentView = new state.view({
            dataSource: this.dataSource,
            navFlow: this,
            stateOptions: stateOptions,
            template: state.template,
            license: this.license
        });
        this.$('.install-steps-pane .inner').html(this._currentView.render().el);

        // highlight progress bar
        this.$('.install-steps-progress .install-step').removeClass('active');
        this.$('.install-steps-progress .install-step[data-step="' + state.indicator + '"]').addClass('active');
    },

    remove: function() {
        WidgetView.prototype.remove.apply(this, arguments);
        if (this._currentView) {
            this._currentView.remove();
        }
    }
    
});

/*
// for dev testing only
window.mockState = {
    LicenseInfoView: {
        defaultLicense: true,
        lastRequest: 'notfound' // exists, notfound, error
    },
    LicenseRegisterView: {
        registerResponse: 'offline' // success, failed, offline, input
    }
};
*/

exports = module.exports = ConfigWelcomeWidget;