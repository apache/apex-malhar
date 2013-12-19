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
 * LogicalOpOverviewWidget
 * 
 * Description of widget.
 *
*/

var _ = require('underscore');
var kt = require('knights-templar');
var BaseView = DT.widgets.OverviewWidget;

// class definition
var LogicalOpOverviewWidget = BaseView.extend({
        
    overview_items: [
    	{ key: 'status', label: DT.text('status_label'), value: function(statuses) {
			return DT.formatters.logicalOpStatusFormatter(statuses);
    	}},
		{ key: 'partitionCount', label: DT.text('num_partitions_label') },
		{ key: 'containerCount', label: DT.text('num_containers_label') },
		{ key: 'tuplesProcessedPSMA', label: DT.text('processed_per_sec'), value: function(v) { return DT.formatters.commaGroups(v.toString()); } },
		{ key: 'totalTuplesProcessed', label: DT.text('processed_total'), value: function(v) { return DT.formatters.commaGroups(v.toString()); } },
		{ key: 'tuplesEmittedPSMA', label: DT.text('emitted_per_sec'), value: function(v) { return DT.formatters.commaGroups(v.toString()); } },
		{ key: 'totalTuplesEmitted', label: DT.text('emitted_total'), value: function(v) { return DT.formatters.commaGroups(v.toString()); } },

		{ key: 'cpuPercentageMA', label: DT.text('cpu_percentage_label'), value: function(cpu) {
			return DT.formatters.percentageFormatter(cpu, true);
		} },
		{ key: 'currentWindowId', label: DT.text('current_wid_label'), title: DT.text('current_wid_title') },
    	{ key: 'recoveryWindowId', label: DT.text('recovery_wid_label'), title: DT.text('recovery_wid_title') },
		{ key: 'failureCount', label: DT.text('failure_count_label') },
		{ key: 'lastHeartbeat', label: DT.text('last_heartbeat_label'), value: function(b) { return new Date(1*b).toLocaleString(); } },
		{ key: 'latencyMA', label: DT.text('latency_ms_label') },
		
    ]
    
});

exports = module.exports = LogicalOpOverviewWidget;