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
var BaseView = require('./StepView');
var Notifier = DT.lib.Notifier;
var ConfigPropertyModel = require('../../../../datatorrent/ConfigPropertyModel');
var ConfigIssueModel = DT.lib.ConfigIssueModel;
var ConfigIssueCollection = DT.lib.ConfigIssueCollection;

var SummaryView = BaseView.extend({

    events: {
        'click .continue': 'continue'
    },

    initialize: function(options) {
        BaseView.prototype.initialize.apply(this, arguments);
        this.dataSource = options.dataSource;
        this.navFlow = options.navFlow;

        this.loading = true;

        this.issues = new ConfigIssueCollection([], { silentErrors: true });

        var issuesPromise = this.issues.fetch();

        issuesPromise.done(function () {
            var configStatusPromise = this.saveConfigStatusProperty();

            configStatusPromise.done(function () {
                this.loading = false;
                /*
                this.issues.add(new ConfigIssueModel({
                    key: 'issue2',
                    severity: 'warning',
                    description: 'description2'
                }));
                this.issues.add(new ConfigIssueModel({
                    key: 'issue3',
                    severity: 'error',
                    description: 'description3'
                }));
                this.issues.add(new ConfigIssueModel({
                    key: 'issue4',
                    severity: 'warning',
                    description: 'description4'
                }));
                */

                this.render();
            }.bind(this));

            configStatusPromise.fail(function () {
                this.showError();
            }.bind(this));
        }.bind(this));

        issuesPromise.fail(function () {
            this.showError();
        }.bind(this));
    },

    showError: function () {
        this.errorMsg = 'Error completing configuration. Please make sure DT Gateway is running.'
        this.render();
    },

    saveConfigStatusProperty: function () {
        var model = new ConfigPropertyModel({
            name: 'dt.configStatus',
            value: 'complete'
        });

        return model.save();
    },

    render: function() {
        var html = this.template({
            errorMsg: this.errorMsg,
            loading: this.loading,
            issues: this.issues
        });

        this.$el.html(html);

        if (this.assignments) {
            this.assign(this.assignments);
        }

        return this;
    },

    assignments: {
    }

});
exports = module.exports = SummaryView;