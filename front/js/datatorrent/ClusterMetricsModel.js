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


var BaseModel = require('./BaseModel');

/**
 * This model holds metrics for a DataTorrent cluster.
 * 
 */
var ClusterMetricsModel = BaseModel.extend({

	debugName: 'Cluster Metrics',

	initialize: function(attrs, options) {

		BaseModel.prototype.initialize.apply(this, arguments);
		
		this.on('sync', function() {
			this.set('as_of', +new Date())
		});
	},

	defaults: {
	    // average running application age in milliseconds
	    averageAge: '',
	    // cpuPercentage
	    cpuPercentage: '',
	    // currentMemoryAllocatedMB
	    currentMemoryAllocatedMB: '',
	    // maxMemoryAllocatedMB
	    maxMemoryAllocatedMB: '',
	    // numAppsFailed
	    numAppsFailed: '',
	    // numAppsFinished
	    numAppsFinished: '',
	    // numAppsKilled
	    numAppsKilled: '',
	    // numAppsPending
	    numAppsPending: '',
	    // numAppsRunning
	    numAppsRunning: '',
	    // numAppsSubmitted
	    numAppsSubmitted: '',
	    // numContainers
	    numContainers: '',
	    // numOperators
	    numOperators: '',
	    // tuplesEmittedPSMA
	    tuplesEmittedPSMA: '',
	    // tuplesProcessedPSMA
	    tuplesProcessedPSMA: '',
	    // last time it was updated
	    as_of: ''
	},

	url: function() {
		return this.resourceURL('ClusterMetrics');
	},

	subscribe: function() {
    	var topic = this.resourceTopic('ClusterMetrics');
    	this.checkForDataSource();
    	this.listenTo(this.dataSource, topic, function(res) {
    		this.set(res);
    	});
    	this.dataSource.subscribe(topic);
    },

    unsubscribe: function() {
    	this.stopListening(this.dataSource);
    }
});

exports = module.exports = ClusterMetricsModel;