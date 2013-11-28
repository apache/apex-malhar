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
 * Logical Operator model
 * 
 * Represents a group of physical operators
 * with the same Logical operator.
**/
var BigInteger = require('jsbn');
var WindowId = require('./WindowId');
var OperatorModel = require('./OperatorModel');
var LogicalOperatorModel = OperatorModel.extend({

	idAttribute: 'logicalName',

	defaults: {
        logicalName: '',
        className: '',
        containers: [],
        cpuPercentageMA: 0,
        currentWindowId: null,     // new WindowId("0")
        recoveryWindowId: null,    // new WindowId("75")
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
	}

});

exports = module.exports = LogicalOperatorModel;