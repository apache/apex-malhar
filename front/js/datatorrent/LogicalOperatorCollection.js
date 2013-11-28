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
/**
 * Logical Operator Collection
 * 
 * Child class of a regular operator class, but
 * groups info by logical operators
 *
**/
var _ = require('underscore');
var WindowId = require('./WindowId');
var BigInteger = require('jsbn');
var OperatorCollection = require('./OperatorCollection');
var LogicalOperatorModel = require('./LogicalOperatorModel');
var LogicalOperatorCollection = OperatorCollection.extend({
	
	debugName: 'Aggregate Operators',

	model: LogicalOperatorModel,

	subscribe: function() {
        var topic = this.resourceTopic('Operators', {
            appId: this.appId
        });
        this.listenTo(this.dataSource, topic, function(data) {
            this.set(this.responseTransform(data));
        });
        this.dataSource.subscribe(topic);
    },

	responseTransform: function(res) {
		
		// Group by the logical name
		var grouped = _.groupBy(res.operators, "logicalName");
		
		// reduce each subset to aggregated logical operator object
		return _.map(grouped, function(group, logicalName) {

			// For now, ignores unifiers until further direction from back-end team
			// Get non-unifiers
			//group = _.filter(group, function(o) { return !(!!o.unifierClass) });

			var reduction = _.reduce(group, function(memo, op, i, list) {

				// Set if operator is a unifier
				var isUnifier = !! op.unifierClass;
                if (!isUnifier) {
                    memo['partitionCount']++;
                }
				
				// container, host
				_.each(['container','host'], function(key) {
					var memo_key = key + 's';
					if (memo[memo_key].indexOf(op[key]) === -1) {
						memo[memo_key].push(op[key]);
					}
				});

				// simple sums
				_.each(["cpuPercentageMA", "failureCount"], function(key) {
					memo[key] = memo[key]*1 + op[key]*1;
				});

				// cur, recovery window
				_.each(['currentWindowId', 'recoveryWindowId'], function(key) {
					var opwindow = new WindowId(op[key]);
					if (memo[key] == null || (opwindow.timestamp*1 <= memo[key].timestamp*1 && opwindow.offset*1 < memo[key].offset*1)) {
						memo[key] = opwindow;
					}
				});

				// ids
				memo.ids.push(op.id);

				// ports
                /*
                if (!isUnifier) {
                    _.each(op.ports, function(port) {
                        memo.ports[port.name].push(port);
                    });
                }
                */

				// lastHeartbeat
				memo.lastHeartbeat = memo.lastHeartbeat === null ? op.lastHeartbeat*1 : Math.min(memo.lastHeartbeat, op.lastHeartbeat*1);

				// latency
				memo.latencyMA = Math.max(memo.latencyMA, op.latencyMA*1);

				// status
				if (memo.status.hasOwnProperty(op.status)) {
					memo.status[op.status].push(op.id);
				} else {
					memo.status[op.status] = [op.id];
				}

				// tuple metrics
				_.each(["totalTuplesEmitted","totalTuplesProcessed","tuplesEmittedPSMA","tuplesProcessedPSMA"], function(key) {
					memo[key] = memo[key].add(new BigInteger(op[key]));
				});

				return memo;
			}, {
				// same
				"className": group[0]["className"], 
				// array of all
		        "containers": [],
		        // sum
		        "cpuPercentageMA": 0,
		        // min
		        "currentWindowId": null,
		        // min
		        "recoveryWindowId": null,
		        // sum
		        "failureCount": 0,
		        // array of all
		        "hosts": [], 
		        // array of all
		        "ids": [],
                ports: [],
                /*
				"ports": (function() {
					var ports = group[0].ports;
					var result = {};
					_.each(ports, function(port) {
						result[port.name] = [];
					});
					return result;
				}()),
				*/

		        // min
		        "lastHeartbeat": null, 
		        // max
		        "latencyMA": 26, 
		        // same
		        "logicalName": group[0]["logicalName"],
		        // map of status => array of ids
		        "status": {}, 
		        // sum
		        "totalTuplesEmitted": BigInteger.ZERO, 
		        "totalTuplesProcessed": BigInteger.ZERO, 
		        "tuplesEmittedPSMA": BigInteger.ZERO, 
		        "tuplesProcessedPSMA": BigInteger.ZERO,
                partitionCount: 0,
                containerCount: 0
			});

            reduction.containerCount = reduction.containers.length;

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
                        "bufferServerBytesPSMA": BigInteger.ZERO,
                        // same
                        "name": name,
                        // sum
                        "totalTuples": BigInteger.ZERO,
                        // sum
                        "tuplesPSMA": BigInteger.ZERO,
                        // same
                        "type": portArray[0].type
                    });
                });
            }
            */

			return reduction;

		}); // end of map
	}

});
exports = module.exports = LogicalOperatorCollection;