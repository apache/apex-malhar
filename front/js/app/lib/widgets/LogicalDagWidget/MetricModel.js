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

var MetricModel = Backbone.Model.extend({

    map: {},
    lastUpdate: 0,
    updateCount: 0,

    initialize: function(attributes, options) {
        this.metricId = options.metricId;
        this.operators = options.operators;
        this.implementation = options.implementation;
    },

    update: function (collection, checkChange) {
        if (this.isNone()) {
            return;
        }

        var newMap = {};
        collection.each(function (model) {
            var id = model.get('logicalName');
            var value = model.get(this.metricId);
            newMap[id] = value;
        }, this);

        var changed = true;

        if (checkChange) {
            changed = !_.isEqual(newMap, this.map)
        }

        this.map = newMap;

        return changed;
    },

    isNone: function () {
        return this.metricId === 'none';
    },

    subscribe: function () {
        this.updateAll();
        this.listenTo(this.operators, 'update', this.updateAll);
    },

    getValue: function (id) {
        return this.map[id];
    },

    getMap: function () {
        return this.map;
    },

    updateAll: function() {
        var newMap = {};
        if (this.implementation.updateAll) {
            this.implementation.updateAll(this.operators, newMap);
        } else {
            this.operators.each(function (operator) {
                var id = operator.get('logicalName');
                this.implementation.update(id, operator, newMap);
            }.bind(this));
        }

        if (!_.isEqual(newMap, this.map)) {
            this.map = newMap;
            this.trigger('change');
        }
    },

    getTextValue: function (id) {
        var value = this.getValue(id);
        if (this.implementation.valueToString) {
            return this.implementation.valueToString(value);
        } else {
            return value;
        }
    },

    showMetric: function (id) {
        return this.implementation.showMetric(id, this.map);
        //return (this.has(id) && value > 0);
    },

    unsubscribe: function () {
        this.stopListening(this.operators);
    }
});

exports = module.exports = MetricModel;