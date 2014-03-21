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
var GatewayInfoModel = require('../../../../datatorrent/GatewayInfoModel');
var Bbind = DT.lib.Bbindings;
var Notifier = DT.lib.Notifier;
var ConfigIssueCollection = DT.lib.ConfigIssueCollection;
var HadoopLocationModel = require('../../../../datatorrent/HadoopLocationModel');
var RestartModal = DT.lib.RestartModal;
var ConfirmModal = require('./ConfirmModalView');

var HadoopView = BaseView.extend({

    events: {
        'click .continue': 'continue'
    },

    initialize: function(options) {
        BaseView.prototype.initialize.apply(this, arguments);
        this.dataSource = options.dataSource;
        this.navFlow = options.navFlow;

        this.error = false; //TODO
        this.loading = true;

        var hadoopLocationPromise = this.loadHadoopLocation();
        var aboutPromise = this.loadAbout();

        var all = $.when(hadoopLocationPromise, aboutPromise);
        all.done(function () {
            this.loading = false;
            this.render();
        }.bind(this));

        all.fail(function () {
            this.errorMsg = 'Internal error. Failed to load installation data.';
            this.render();
        }.bind(this));

        this.subview('hadoop-location', new Bbind.text({
            model: this.hadoopLocationModel,
            attr: 'value',
            updateEvents: ['blur'],
            clearErrorOnFocus: true,
            listenToModel: false,
            setAnyway: true,
            classElement: function($el) {
                return $el.parent().parent();
            },
            errorEl: '.help-block',
            errorClass: 'error'
        }));

        this.listenTo(this.hadoopLocationModel, 'change', function () {
            this.clearError('.hadoop-error');
        });
    },

    inputChanged: function () {
        var hadoopLocationModelValid = this.hadoopLocationModel.isValid();

        if (hadoopLocationModelValid) {
            this.$el.find('.continue').removeClass('disabled');
        } else {
            this.$el.find('.continue').addClass('disabled');
        }
    },

    loadDfsProperty: function () {
        var promise = this.loadProperty('dt.dfsRootDirectory');

        this.dfsDirectory = '';

        promise.then(function (data) {
            if (data && data.value) {
                this.dfsDirectory = data.value;
            }
        }.bind(this));

        return promise;
    },

    saveHadoopLocation: function () {
        var d = $.Deferred();

        //setTimeout(function () { d.rejectWith(null, ['hadoop location update failed test msg']); }, 3000); return d.promise();

        var ajax = this.hadoopLocationModel.save();

        ajax.done(function () {
            d.resolve();
        }.bind(this));

        ajax.fail(function (jqXHR) {
            var response = JSON.parse(jqXHR.responseText);
            d.rejectWith(null, [response.message]);
        }.bind(this));

        return d.promise();
    },

    loadHadoopLocation: function () {
        var d = $.Deferred();

        this.hadoopLocationModel = new HadoopLocationModel();
        var ajax = this.hadoopLocationModel.fetch();

        ajax.done(function () {
            // save default value
            this.hadoopLocationModel.init(this.hadoopLocationModel.get('value'));
            d.resolve();
        }.bind(this));

        ajax.fail(function (jqXHR) {
            if (jqXHR.status === 404) { //TODO
                this.hadoopLocationModel.init('');
                d.resolve();
            } else {
                d.reject();
            }
        }.bind(this));

        return d.promise();
    },

    loadAbout: function () {
        var d = $.Deferred();

        this.about = new GatewayInfoModel({}, { silentErrors: true });
        this.about.fetch(); //TODO error handling

        this.listenTo(this.about, 'sync', function () {
            d.resolve();
        });

        return d.promise();
    },

    continue: function (event) {
        event.preventDefault();

        if (jQuery(event.target).hasClass('disabled')) {
            return;
        }

        this.$el.find('.hadoop-location').blur();

        if (!this.hadoopLocationModel.isValid()) {
            return;
        }

        this.$el.find('.loading').show();
        this.$el.find('.continue').addClass('disabled');

        var hadoopLocationPromise;
        if (this.hadoopLocationModel.isChanged()) {
            hadoopLocationPromise = this.saveHadoopLocation();
        } else {
            hadoopLocationPromise = this.createResolvedPromise();
        }

        hadoopLocationPromise.fail(function (msg) {
            this.$el.find('.loading').hide();
            this.$el.find('.continue').removeClass('disabled');
            this.showError('.hadoop-error', msg);
        }.bind(this));

        hadoopLocationPromise.done(function () {
            var issues = new ConfigIssueCollection([], { silentErrors: true });
            var issuesPromise = issues.fetch();

            issuesPromise.done(function () {
                this.$el.find('.loading').hide();
                this.$el.find('.continue').removeClass('disabled');

                var restartRequiredIssue = issues.findWhere({
                    key: 'RESTART_NEEDED'
                });

                var restartRequired = !!restartRequiredIssue;

                //if (true) {
                if (restartRequired) {
                    var modal = new ConfirmModal({
                        message: 'Changes made require restart. Please restart the Gateway.',
                        confirmCallback: this.restart.bind(this)
                    });
                    modal.addToDOM();
                    modal.launch();
                } else {
                    this.navFlow.go('SystemView');
                }
            }.bind(this));

            issuesPromise.fail(function () {
                this.errorMsg = 'Internal Error. Failed to load installation status.';
                this.render();
            }.bind(this));
        }.bind(this));
    },

    restart: function () {
        var restartModal = new RestartModal({
            dataSource: this.dataSource,
            message: 'Restarting the Gateway...',
            restartCompleteCallback: this.restartComplete.bind(this)
        });
        restartModal.addToDOM();
        restartModal.launch();
    },

    restartComplete: function () {
        this.navFlow.go('SystemView');
    },

    createResolvedPromise: function () {
        var d = $.Deferred();
        d.resolve();
        return d.promise();
    },

    showError: function (selector, msg) {
        var el = this.$el.find(selector);
        el.text(msg);
        el.show();
    },

    clearError: function (selector) {
        this.$el.find(selector).hide();
    },

    render: function() {
        var html = this.template({
            errorMsg: this.errorMsg,
            loading: this.loading,
            hadoopLocationModel: this.hadoopLocationModel,
            about: this.about
        });

        this.$el.html(html);

        if (this.assignments) {
            this.assign(this.assignments);
        }

        return this;
    },

    assignments: {
        '.hadoop-location': 'hadoop-location'
    }

});
exports = module.exports = HadoopView;