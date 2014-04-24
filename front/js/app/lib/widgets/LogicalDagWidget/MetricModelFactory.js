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
var MetricModel = require('./MetricModel');
var BigInteger = require('jsbn');
var formatters = DT.formatters;
var bormat = require('bormat');

var MetricModelFactory = {
    getMetricModel: function (metricId) {
        return new MetricModel(null, {
            metricId: metricId,
            implementation: this.metrics[metricId]
        });
    },
    metrics: {
        none: {
            showMetric: function (id, map) {
                return false;
            }
        },

        tuplesProcessedPSMA: {
            showMetric: function (id, map) {
                var value = map[id];
                return (map.hasOwnProperty(id) && value > 0);
            },

            valueToString: function (value) {
                return bormat.commaGroups(value);
            }
        },

        tuplesEmittedPSMA: {
            showMetric: function (id, map) {
                var value = map[id];
                return (map.hasOwnProperty(id) && value > 0);
            },

            valueToString: function (value) {
                return bormat.commaGroups(value);
            }
        },

        latencyMA: {
            showMetric: function (id, map) {
                var value = map[id];
                return (map.hasOwnProperty(id) && value > 0);
            }
        },

        partitionCount: {
            showMetric: function (id, map) {
                var value = map[id];
                return (map.hasOwnProperty(id) && value > 1);
            }
        },

        containerCount: {
            showMetric: function (id, map) {
                return map.hasOwnProperty(id);
            }
        },

        cpuMin: {
            showMetric: function (id, map) {
                return map.hasOwnProperty(id);
            },

            valueToString: function (value) {
                return formatters.percentageFormatter(value, true);
            }
        },

        cpuMax: {
            showMetric: function (id, map) {
                return map.hasOwnProperty(id);
            },

            valueToString: function (value) {
                return formatters.percentageFormatter(value, true);
            }
        },

        cpuAvg: {
            showMetric: function (id, map) {
                return map.hasOwnProperty(id);
            },

            valueToString: function (value) {
                return formatters.percentageFormatter(value, true);
            }
        },

        lastHeartbeat: {
            showMetric: function (id, map) {
                return map.hasOwnProperty(id);
            },

            valueToString: function (value) {
                return new Date(value).toLocaleTimeString();
            }
        },

        currentWindowId: {
            showMetric: function (id, map) {
                return map.hasOwnProperty(id);
            },

            valueToString: function (value) {
                return formatters.windowOffsetFormatter(value);
            }
        },

        totalTuplesProcessed: {
            showMetric: function (id, map) {
                var value = map[id];
                return (map.hasOwnProperty(id) && value > 0);
            },

            valueToString: function (value) {
                return bormat.commaGroups(value);
            }
        },

        totalTuplesEmitted: {
            showMetric: function (id, map) {
                var value = map[id];
                return (map.hasOwnProperty(id) && value > 0);
            },

            valueToString: function (value) {
                return bormat.commaGroups(value);
            }
        },
        recoveryWindowId: {
          showMetric: function (id, map) {
            return map.hasOwnProperty(id);
          },

          valueToString: function (value) {
            return formatters.windowOffsetFormatter(value);
          }
        }
    }
};

exports = module.exports = MetricModelFactory;