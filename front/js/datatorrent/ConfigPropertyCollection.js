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
var BaseCollection = require('./BaseCollection');
var ConfigPropertyModel = require('./ConfigPropertyModel');

var ConfigPropertyCollection = BaseCollection.extend({

    debugName: 'config properties',

    model: ConfigPropertyModel,

    url: function() {
        return this.resourceURL('ConfigProperty');
    },

    responseTransform: function(response) {
        var result = [];
        _.each(response, function(obj, name) {
            obj.name = name;
            result.push(obj);
        });
        return result;
    },

    setIssues: function(issues) {
        if (this.issues) {
            this.stopListening(this.issues);
        }
        this.issues = issues;
        this.updateIssues();
        this.listenTo(this.issues, 'change', this.updateIssues);
    },

    updateIssues: function() {
        if (this.issues) {

            // unset all issues
            this.each(function(prop) {
                prop.unset('issue');
            });

            // look for issues to update
            this.issues.each(function(issue) {
                var propertyName, property;
                // Check if issue pertains to a property
                if (propertyName = issue.get('property')) {
                    property = this.get(propertyName);
                    if (property) {
                        property.set('issue', issue.toJSON());
                    }
                }
            }, this);
        }
    }

});

exports = module.exports = ConfigPropertyCollection;