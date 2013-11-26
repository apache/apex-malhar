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
var Notifier = require('./Notifier');
var BaseModel = require('./BaseModel');
var path = require('path');
var settings = require('./settings');
var AlertTemplateModel = require('./AlertTemplateModel');
var AlertParamsModel = require('./AlertParamsModel');
var AlertModel = BaseModel.extend({

    idAttribute: 'name',

    defaults: {
        appId: '',
        name: '',
        operatorName: '',
        streamName: '',
        templateName: '',
        lastTriggered: '',
        parameters: new AlertParamsModel({})
    },

    urlRoot: function() {
        return this.resourceURL('Alert', {
            appId: this.get('appId') || this.collection.appId
        });
    },
    
    validate: function(attrs) {
        var invalid = {},
            parameters = this.get('parameters'),
            invalidParams;
        
        if (!(/^[a-zA-Z0-9_]+$/.test(attrs.name))) {
            invalid.name = 'Alert names may only contain letters, numbers, and underscores.';
        }
        if (!attrs.templateName) {
            invalid.templateName = 'A template is required for an alert';
        }
        if (!attrs.streamName) {
            invalid.streamName = 'A stream must be specified for an alert';
        }
        if (!attrs.operatorName) {
            invalid.operatorName = 'An operator must be specified';
        }
        
        if (!parameters.isValid()) {
            _.each(parameters.validationError, function(msg, key) {
                invalid['parameters.' + key] = msg;
            });
        }

        if (! _.isEmpty(invalid) ) {
            return invalid;
        }
    },
    
    toJSON: function() {
        var json = _.clone(this.attributes);
        
        if (_(json.parameters.toJSON).isFunction()) {
            json.parameters = json.parameters.toJSON();
        }
        return json;
    },

    create: function() {
        var json = this.toJSON();
        var data = {};
        
        _.each(['parameters', 'streamName', 'templateName'], function(key) {
            data[key] = json[key];
        });
        
        return this.save({}, {
            contentType: 'application/json',
            data: JSON.stringify(data),
            success: function(model, response, options) {
                Notifier.success({
                    'title': 'Alert Created',
                    'text': 'Alert successfully created.'
                });
            }.bind(this),
            error: function(model, xhr, options) {
                var emsg = 'Alert Creation Failed';
                Notifier.error({
                    'title': emsg,
                    'text': 'Error creating an alert. Status: ' + xhr.status + '.'
                });
                throw new Error(emsg + '. URL: ' + options.url);
            }
        });
    },
    
    delete: function() {
        var alertName = this.get('name');

        if (!this.collection || !this.collection.appId) {
            throw new Error('Alert cannot be deleted. No app id for alert ' + alertName);
        }

        this.destroy({
            wait: true,
            success: function(model, response, options) {
                this.trigger('destroy', this, this.collection);
                Notifier.success({
                    'title': 'Alert Deleted',
                    'text': 'Alert ' + alertName + ' has been successfully deleted.'
                });
            }.bind(this),
            error: function(model, xhr, options) {
                var emsg = 'Alerts Deletion Failed';
                Notifier.error({
                    'title': emsg,
                    'text': 'Error deleting alert. Status: ' + xhr.status + '.'
                });
            }
        });
    }
});
exports = module.exports = AlertModel;