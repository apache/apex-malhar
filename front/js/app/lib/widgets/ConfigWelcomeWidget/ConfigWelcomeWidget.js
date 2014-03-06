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
var StepCollection = require('./StepCollection');
var steps = require('./steps');
var StepListView = require('./StepListView');
var StepView = require('./StepView');
var SystemView = require('./SystemView');
var LicenseInfoView = require('./LicenseInfoView');
var LicenseRegisterView = require('./LicenseRegisterView');
var LicenseUploadView = require('./LicenseUploadView');
var LicenseOfflineView = require('./LicenseOfflineView');
var SummaryView = require('./SummaryView');

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

        // installation issues, instantiated 
        // and fetched on WelcomePageView
        this.issues = options.issues;

        // models for each step in this welcome
        // process.
        this.steps = new StepCollection(steps);
        //this.steps.setActive('welcome');
        //this.steps.setActive('system');

        // this view contains the list of steps on the left
        // side of the wizard. 
        this.subview('step_list', new StepListView({
            collection: this.steps
        }));

        // when active step changes, update the main 
        // pane with the appropriate step template.
        this.listenTo(this.steps, 'change:active', function(model, active) {
            // this catches change events when steps
            // get deactivated; we are only interested
            // in steps becoming active
            if (!active) {
                return;
            }
            this.goToStep(model);
        });

        this.activeStateId = 'WelcomeView';
        //this.activeStateId = 'LicenseInfoView';
        //this.activeStateId = 'LicenseRegisterView';
        //this.activeStateId = 'LicenseOfflineView';
        //this.activeStateId = 'LicenseUploadView';
        //this.activeStateId = 'SystemView';
    },

    render: function() {
        //this.mockState = window.mockState; //TODO

        // Sets up the base markup for the wizard
        var html = this.template({});
        this.$el.html(html);

        // Assigns the step_list to the <ol>
        this.assign({
            'ol.step-list': 'step_list'
        });

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

    goToStep: function(step) {
        if (true) {
            this.go(step);
            return;
        }
        // Retrieves template for the step
        var stepId = step.get('id');
        var StepView = this.stepViews[stepId];
        var template = this.stepTemplates[stepId];
            
        if (!StepView) {
            throw new Error('No view found for "' + step.get('id') + '" step!');
        }

        if (!template) {
            throw new Error('No template found for "' + step.get('id') + '" step!');
        }

        // Remove old current view if present
        if (this._currentView) {
            this._currentView.remove();
        }

        // Injects the issues, properties, and step model
        // into the new view.
        this._currentView = new StepView({
            dataSource: this.dataSource,
            issues: this.issues,
            properties: this.collection,
            model: step,
            template: template
        });
        this.$('.install-steps-pane .inner').html(this._currentView.render().el);
    },
    
    // base markup for the wizard
    template: kt.make(__dirname+'/ConfigWelcomeWidget.html','_'),

    // markup for each step
    stepTemplates: {
        welcome: kt.make(__dirname+'/step_welcome.html'),
        system: kt.make(__dirname+'/SystemView.html'),
        performance: kt.make(__dirname+'/step_performance.html'),
        applications: kt.make(__dirname+'/step_applications.html'),
        summary: kt.make(__dirname+'/step_summary.html')
    },

    // Views for each step
    stepViews: {
        welcome: StepView,
        system: SystemView,
        performance: StepView,
        applications: StepView,
        summary: StepView
    },

    navStates: {
        WelcomeView: {
            view: StepView, //TODO
            template: kt.make(__dirname+'/step_welcome.html') //TODO
        },
        LicenseInfoView: {
            view: LicenseInfoView,
            template: kt.make(__dirname+'/LicenseInfoView.html')
        },
        LicenseRegisterView: {
            view: LicenseRegisterView,
            template: kt.make(__dirname+'/LicenseRegisterView.html')
        },
        LicenseUploadView: {
            view: LicenseUploadView,
            template: kt.make(__dirname+'/LicenseUploadView.html')
        },
        LicenseOfflineView: {
            view: LicenseOfflineView,
            template: kt.make(__dirname+'/LicenseOfflineView.html')
        },
        SystemView: {
            view: SystemView,
            template: kt.make(__dirname+'/SystemView.html') // TODO
        },
        SummaryView: {
            view: SummaryView,
            template: kt.make(__dirname+'/SummaryView.html')
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
        var that = this;
        this._currentView = new state.view({
            dataSource: this.dataSource,
            navFlow: that,
            stateOptions: stateOptions,
            template: state.template
        });
        this.$('.install-steps-pane .inner').html(this._currentView.render().el);
    },

    remove: function() {
        WidgetView.prototype.remove.apply(this, arguments);
        if (this._currentView) {
            this._currentView.remove();
        }
    }
    
});

//TODO
window.mockState = {
    LicenseInfoView: {
        defaultLicense: true,
        lastRequest: 'notfound' // exists, notfound, error
    },
    LicenseRegisterView: {
        registerResponse: 'offline' // success, failed, offline, input
    }
};

exports = module.exports = ConfigWelcomeWidget;