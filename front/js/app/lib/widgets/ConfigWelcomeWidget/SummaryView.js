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
var ConfigIssueCollection = DT.lib.ConfigIssueCollection;
var Notifier = DT.lib.Notifier;
var RestartModal = DT.lib.RestartModal;
var ConfigPropertyModel = require('../../../../datatorrent/ConfigPropertyModel');

var SummaryView = BaseView.extend({

    events: {
        'click .continue': 'continue'
    },

    initialize: function(options) {
        BaseView.prototype.initialize.apply(this, arguments);
        this.dataSource = options.dataSource;
        this.navFlow = options.navFlow;

        this.error = false; //TODO
        this.loading = true;

        this.issues = new ConfigIssueCollection([]);

        var issuesPromise = this.issues.fetch();
        var configStatusPromise = this.saveConfigStatusProperty();
        //var d = $.Deferred();
        //setTimeout(function () {
        //    d.reject();
        //}, 3000);
        //var ajax = d.promise();

        var all = $.when(issuesPromise, configStatusPromise);

        all.done(function () {
            this.loading = false;

            var restartRequiredIssue = this.issues.findWhere({
                key: 'RESTART_NEEDED'
            });

            var restartRequired = !!restartRequiredIssue;

            //if (true) {
            if (restartRequired) {
                var restartModal = new RestartModal({
                    dataSource: this.dataSource,
                    message: 'Changes made require restart. Restarting the Gateway...'
                });
                restartModal.addToDOM();
                restartModal.launch();
            }

            this.render();
        }.bind(this));

        all.fail(function () {
            this.error = true;
            this.render();
        }.bind(this));
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
            error: this.error,
            loading: this.loading,
            restartRequired: this.restartRequired
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