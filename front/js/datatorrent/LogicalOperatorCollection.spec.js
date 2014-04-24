/*
* Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
*
* Licensed under the Apache License, Version 2.0 (the 'License');
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*   http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an 'AS IS' BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
var BigInteger = require('jsbn');
var Collection = require('./LogicalOperatorCollection');
var WindowId = require('./WindowId');
describe('LogicalOperatorCollection.js', function() {
    
	var sandbox, lastHeartbeat, operators, response;

	beforeEach(function() {
	    sandbox = sinon.sandbox.create();
	    lastHeartbeat = +new Date() - 500;
	    operators = [
	    	// operator1
			{
			    className: 'com/datatorrent/lib/operator1', 
			    container: '1', 
			    cpuPercentageMA: '10', 
			    currentWindowId: '98',
			    recoveryWindowId: '74',
			    failureCount: '1',
			    host: 'node0.morado.com', 
			    id: '1', 
			    ports: [{
			    	bufferServerBytesPSMA: '1000',
					name: 'integer_data',
					recordingStartTime: '-1',
					totalTuples: '100',
					tuplesPSMA: '10',
					type: 'output'
			    }],
			    lastHeartbeat: lastHeartbeat, 
			    latencyMA: '24', 
			    logicalName: 'op1',
			    recordingStartTime: '-1',
			    status: 'ACTIVE', 
			    totalTuplesEmitted: '1000', 
			    totalTuplesProcessed: '1000', 
			    tuplesEmittedPSMA: '10', 
			    tuplesProcessedPSMA: '10',
			},
		    {
		        className: 'com/datatorrent/lib/operator1', 
		        container: '2', 
		        cpuPercentageMA: '9', 
		        currentWindowId: '99',
		        recoveryWindowId: '75',
		        failureCount: '0', 
		        host: 'node1.morado.com', 
		        id: '2', 
		        ports: [{
		        	bufferServerBytesPSMA: '900',
					name: 'integer_data',
					recordingStartTime: '-1',
					totalTuples: '110',
					tuplesPSMA: '11',
					type: 'output'
		        }], 
		        lastHeartbeat: lastHeartbeat -1, 
		        latencyMA: '25', 
		        logicalName: 'op1',
		        recordingStartTime: '-1',
		        status: 'ACTIVE', 
		        totalTuplesEmitted: '1100', 
		        totalTuplesProcessed: '1100', 
		        tuplesEmittedPSMA: '11', 
		        tuplesProcessedPSMA: '11'
		    },
		    {
		        className: 'com/datatorrent/lib/operator1', 
		        container: '3', 
		        cpuPercentageMA: '11', 
		        currentWindowId: '100',
		        recoveryWindowId: '75',
		        failureCount: '3', 
		        host: 'node1.morado.com', 
		        id: '3', 
		        ports: [{
		        	bufferServerBytesPSMA: '1100',
					name: 'integer_data',
					recordingStartTime: '-1',
					totalTuples: '90',
					tuplesPSMA: '9',
					type: 'output'
		        }],
		        lastHeartbeat: lastHeartbeat -2, 
		        latencyMA: '26', 
		        logicalName: 'op1',
		        recordingStartTime: '-1',
		        status: 'INACTIVE', 
		        totalTuplesEmitted: '900', 
		        totalTuplesProcessed: '900', 
		        tuplesEmittedPSMA: '9', 
		        tuplesProcessedPSMA: '9'
		    },
		    // operator2
		    {
		        className: 'com/datatorrent/lib/operator2', 
		        container: '3', 
		        cpuPercentageMA: '15', 
		        currentWindowId: '99',
		        recoveryWindowId: '75',
		        failureCount: '1', 
		        host: 'node2.morado.com', 
		        id: '4', 
		        ports: [
		        	{
		        		bufferServerBytesPSMA: '100',
						name: 'port1',
						recordingStartTime: '-1',
						totalTuples: '200',
						tuplesPSMA: '20',
						type: 'input'
		        	},
		        	{
		        		bufferServerBytesPSMA: '50',
						name: 'port2',
						recordingStartTime: '-1',
						totalTuples: '300',
						tuplesPSMA: '30',
						type: 'output'
		        	}
		        ],
		        lastHeartbeat: lastHeartbeat, 
		        latencyMA: '24', 
		        logicalName: 'op2',
		        recordingStartTime: '-1',
		        status: 'ACTIVE', 
		        totalTuplesEmitted: '1200', 
		        totalTuplesProcessed: '1200', 
		        tuplesEmittedPSMA: '12', 
		        tuplesProcessedPSMA: '12'
		    },
		    {
		        className: 'com/datatorrent/lib/operator2', 
		        container: '4', 
		        cpuPercentageMA: '13', 
		        currentWindowId: '100',
		        recoveryWindowId: '76',
		        failureCount: '0', 
		        host: 'node2.morado.com', 
		        id: '5', 
		        ports: [
		        	{
		        		bufferServerBytesPSMA: '100',
						name: 'port1',
						recordingStartTime: '-1',
						totalTuples: '200',
						tuplesPSMA: '20',
						type: 'input'
		        	},
		        	{
		        		bufferServerBytesPSMA: '50',
						name: 'port2',
						recordingStartTime: '-1',
						totalTuples: '300',
						tuplesPSMA: '30',
						type: 'output'
		        	}
		        ], 
		        lastHeartbeat: lastHeartbeat - 3, 
		        latencyMA: '24', 
		        logicalName: 'op2',
		        recordingStartTime: '-1',
		        status: 'INACTIVE', 
		        totalTuplesEmitted: '1000',
		        totalTuplesProcessed: '1000',
		        tuplesEmittedPSMA: '10', 
		        tuplesProcessedPSMA: '10'
		    }
	    ]
		
		response = Collection.prototype.responseTransform({ operators: operators });
	});

	afterEach(function() {
	    sandbox.restore();
	});

	it('should return an array', function() {
    	expect(response).to.be.instanceof(Array);
	});

	it('should not throw if operators is empty', function() {
	  	var fn = function() {
	  		response = Collection.prototype.responseTransform({ operators: [] });
	  	};
	  	expect(fn).not.to.throw();
	});

	it('should return an empty array', function() {
	  	response = Collection.prototype.responseTransform({ operators: [] });
	  	expect(response).to.eql([]);
	});

	it('should keep the same class name', function() {
	    expect(response[0].className).to.equal('com/datatorrent/lib/operator1');
	    expect(response[1].className).to.equal('com/datatorrent/lib/operator2');
	});

	it('should create an array of all containers spanned over logical operator', function() {
	    expect(response[0].containers).to.eql(['1','2','3']);
	    expect(response[1].containers).to.eql(['3','4']);
	});

	it('should sum the cpuPercentageMA', function() {
	    expect(response[0].cpuSum).to.equal(30);
	    expect(response[1].cpuSum).to.equal(28);
	});

	it('should take the min of cpuPercentageMA', function() {
	  	expect(response[0].cpuMin).to.equal(9);
	  	expect(response[1].cpuMin).to.equal(13);
	});

	it('should take the max of cpuPercentageMA', function() {
	  	expect(response[0].cpuMax).to.equal(11);
	  	expect(response[1].cpuMax).to.equal(15);
	});

	it('should take the average of cpuPercentageMA', function() {
	  	expect(response[0].cpuAvg).to.equal(10);
	  	expect(response[1].cpuAvg).to.equal(14);	
	});

	it('should take the min of the current window', function() {
	    expect(response[0].currentWindowId).to.eql(new WindowId('98'));
	    expect(response[1].currentWindowId).to.eql(new WindowId('99'));
	});

	it('should sum the failureCount', function() {
	   	expect(response[0].failureCount).to.equal(4);
	    expect(response[1].failureCount).to.equal(1); 
	});

	it('should create an array of all hosts', function() {
	    expect(response[0].hosts).to.eql(['node0.morado.com','node1.morado.com']);
	    expect(response[1].hosts).to.eql(['node2.morado.com']);
	});

	it('should create an array of all ids of physical operators', function() {
		expect(response[0].ids).to.eql(['1','2','3']);
	    expect(response[1].ids).to.eql(['4','5']);
	});

	it('should take the min of the lastHeartbeat', function() {
	    expect(response[0].lastHeartbeat).to.equal(lastHeartbeat - 2);
	    expect(response[1].lastHeartbeat).to.equal(lastHeartbeat - 3);
	});

	it('should take the max of latency', function() {
		expect(response[0].latencyMA).to.equal(26);
	    expect(response[1].latencyMA).to.equal(24); 
	});

	it('should keep the logicalName the same', function() {
	    expect(response[0].logicalName).to.equal('op1');
	    expect(response[1].logicalName).to.equal('op2'); 
	});

	it('should create a map of statuses from all physical operators', function() {
	    expect(response[0].status).to.eql({'ACTIVE': ['1', '2'], 'INACTIVE': ['3'] });
	    expect(response[1].status).to.eql({'ACTIVE': ['4'], 'INACTIVE': ['5'] });
	});

	it('should sum the totalTuplesEmitted', function() {
	    expect(response[0].totalTuplesEmitted).to.eql(new BigInteger('3000'));
	    expect(response[1].totalTuplesEmitted).to.eql(new BigInteger('2200'));
	});

	it('should sum the totalTuplesProcessed', function() {
	    expect(response[0].totalTuplesProcessed).to.eql(new BigInteger('3000'));
	    expect(response[1].totalTuplesProcessed).to.eql(new BigInteger('2200'));
	});

	it('should sum the tuplesEmittedPSMA', function() {
	    expect(response[0].tuplesEmittedPSMA).to.eql(new BigInteger('30'));
	    expect(response[1].tuplesEmittedPSMA).to.eql(new BigInteger('22'));
	});

	it('should sum the tuplesProcessedPSMA', function() {
	    expect(response[0].tuplesProcessedPSMA).to.eql(new BigInteger('30'));
	    expect(response[1].tuplesProcessedPSMA).to.eql(new BigInteger('22'));
	});

	it('should calculate the partitionCount', function() {
	    expect(response[0].partitionCount).to.equal(3);
	    expect(response[1].partitionCount).to.equal(2);
	});

	it('should calculate the containerCount', function() {
	   	expect(response[0].containerCount).to.equal(3);
	    expect(response[1].containerCount).to.equal(2); 
	});

	// it('should aggregate the ports', function() {
	//     expect(response[0].ports).to.eql([{
    //     	// sum
    //     	'bufferServerBytesPSMA': new BigInteger('3000'),
    //     	// same
	// 		'name': 'integer_data',
	// 		// sum
	// 		'totalTuples': new BigInteger('300'),
	// 		// sum
	// 		'tuplesPSMA': new BigInteger('30'),
	// 		// same
	// 		'type': 'output'
    //     }]);
	// 
    //     expect(response[1].ports).to.eql([
    //     	{
    //     		// sum
    //     		'bufferServerBytesPSMA': new BigInteger('200'),
    //     		// same
	// 			'name': 'port1',
	// 			// sum
	// 			'totalTuples': new BigInteger('400'),
	// 			// sum
	// 			'tuplesPSMA': new BigInteger('40'),
	// 			// same
	// 			'type': 'input'
    //     	},
    //     	{
    //     		'bufferServerBytesPSMA': new BigInteger('100'),
	// 			'name': 'port2',
	// 			'totalTuples': new BigInteger('600'),
	// 			'tuplesPSMA': new BigInteger('60'),
	// 			'type': 'output'
    //     	}
    //     ]);
	// });

});