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
var BigInteger = require('jsbn');
var WindowId = require('./WindowId');
var OperatorModel = require('./OperatorModel');
var OperatorCollection = require('./OperatorCollection');

/**
 * Logical Operator model
 * 
 * Represents a group of physical operators
 * with the same Logical operator.
**/
var LogicalOperatorModel = OperatorModel.extend({

    idAttribute: 'logicalName',

    defaults: {
        appId: '',
        logicalName: '',
        className: '',
        containers: [],
        cpuPercentageMA: 0,
        cpuMin: 0,
        cpuMax: 0,
        cpuAvg: 0,
        currentWindowId: 0,     // new WindowId('0')
        recoveryWindowId: 0,    // new WindowId('75')
        failureCount: 1,
        hosts: [],
        ids: [],
        ports: [],
        lastHeartbeat: 0,
        latencyMA: 0,
        status: {},
        totalTuplesEmitted: BigInteger.ZERO,
        totalTuplesProcessed: BigInteger.ZERO,
        tuplesEmittedPSMA: BigInteger.ZERO,
        tuplesProcessedPSMA: BigInteger.ZERO,
        partitionCount: 0,
        containerCount: 0
    },

    initialize: function(attrs, options) {
        OperatorModel.prototype.initialize.call(this, attrs, options);

        if (options.keepPhysicalCollection) {
            this.physicalOperators = new OperatorCollection([], {
                dataSource: this.dataSource
            });
            this.physicalOperators.appId = this.get('appId');
        }
    },

    url: function() {
        return this.resourceURL('Operator', {
            appId: this.get('appId')
        });
    },

    subscribe: function() {
        this.checkForDataSource();
        var topic = this.resourceTopic('Operators', {
            appId: this.get('appId')
        });
        this.listenTo(this.dataSource, topic, function(data) {
            this.set(this.responseTransform(data));
            this.trigger('update');
        });
        this.dataSource.subscribe(topic);
    },

    responseTransform: function(res) {
        var operators = _.filter(res.operators, function(op) {
            return op.logicalName === this.get('logicalName');
        }, this);

        if (this.physicalOperators) {
            this.physicalOperators.set(operators);
        }

        var transformed = this.reducePhysicalOperators(operators, this.get('logicalName'));
        return transformed;
    },

    reducePhysicalOperators: function(group, logicalName) {

        // For now, ignores unifiers until further direction from back-end team
        // Get non-unifiers
        group = _.filter(group, function(o) {
            return !(!!o.unifierClass);
        });
        var reduction = _.reduce(group, function(memo, op, i, list) {

            var cpuPercentageMA = op.cpuPercentageMA*1;

            memo.ids.push(op.id);
            memo.lastHeartbeat = Math.min(memo.lastHeartbeat, op.lastHeartbeat*1);
            memo.latencyMA = Math.max(memo.latencyMA, op.latencyMA*1);
            memo.cpuMin = Math.min(memo.cpuMin, cpuPercentageMA);
            memo.cpuMax = Math.max(memo.cpuMax, cpuPercentageMA);
            memo.cpuAvg += cpuPercentageMA;
            memo.cpuSum += cpuPercentageMA;

            // status
            if (memo.status.hasOwnProperty(op.status)) {
                memo.status[op.status].push(op.id);
            } else {
                memo.status[op.status] = [op.id];
            }
            // simple sums
            _.each(['cpuPercentageMA','failureCount'], function(key) {
                memo[key] = memo[key]*1 + op[key]*1;
            });
            // tuple metrics
            _.each(['totalTuplesEmitted','totalTuplesProcessed','tuplesEmittedPSMA','tuplesProcessedPSMA'], function(key) {
                memo[key] = memo[key].add(new BigInteger(op[key]));
            });
            // container, host
            _.each(['container','host'], function(key) {
                var memo_key = key + 's';
                if (memo[memo_key].indexOf(op[key]) === -1) {
                    memo[memo_key].push(op[key]);
                }
            });
            // cur, recovery window (minimum)
            _.each(['currentWindowId', 'recoveryWindowId'], function(key) {
                var opwindow = new WindowId(op[key]);
                if (memo[key] === null || (opwindow.timestamp*1 <= memo[key].timestamp*1 && opwindow.offset*1 < memo[key].offset*1)) {
                    memo[key] = opwindow;
                }
            });

            // ports
            /*
            _.each(op.ports, function(port) {
                memo.ports[port.name].push(port);
            });
            */

            return memo;
        }, {
            // same
            className: group[0].className, 
            // array of all
            containers: [],
            // cpu metrics
            cpuSum: 0,
            cpuMin: group[0].cpuPercentageMA*1,
            cpuMax: group[0].cpuPercentageMA*1,
            cpuAvg: 0,
            // min
            currentWindowId: null,
            // min
            recoveryWindowId: null,
            // sum
            failureCount: 0,
            // array of all
            hosts: [], 
            // array of all
            ids: [],
            ports: [],
            /*
            ports: (function() {
                var ports = group[0].ports;
                var result = {};
                _.each(ports, function(port) {
                    result[port.name] = [];
                });
                return result;
            }()),
            */

            // min
            lastHeartbeat: group[0].lastHeartbeat, 
            // max
            latencyMA: group[0].latencyMA,
            // same
            logicalName: group[0].logicalName,
            // map of status => array of ids
            status: {}, 
            // sum
            totalTuplesEmitted: BigInteger.ZERO, 
            totalTuplesProcessed: BigInteger.ZERO, 
            tuplesEmittedPSMA: BigInteger.ZERO, 
            tuplesProcessedPSMA: BigInteger.ZERO,
            partitionCount: 0,
            containerCount: 0
        });
        var partitionCount = reduction.ids.length;
        reduction.cpuAvg /= partitionCount;
        reduction.containerCount = reduction.containers.length;
        reduction.partitionCount = partitionCount;

        // Reduce ports
        /*
        if (reduction.ports.length) {
            reduction.ports = _.map(reduction.ports, function(portArray, name, list) {

                return _.reduce(portArray, function(memo, port, i, list) {
                    _.each(['bufferServerBytesPSMA', 'totalTuples', 'tuplesPSMA'], function(key) {
                        memo[key] = memo[key].add(new BigInteger(port[key]));
                    });
                    return memo;
                },{
                    // sum
                    'bufferServerBytesPSMA': BigInteger.ZERO,
                    // same
                    'name': name,
                    // sum
                    'totalTuples': BigInteger.ZERO,
                    // sum
                    'tuplesPSMA': BigInteger.ZERO,
                    // same
                    'type': portArray[0].type
                });
            });
        }
        */

        return reduction;

    }

});

exports = module.exports = LogicalOperatorModel;