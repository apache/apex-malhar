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
 * Overview widget for app instances
 * 
*/
var _ = require('underscore');
var formatters = DT.formatters;
var kt = require('knights-templar');
var BaseView = DT.widgets.OverviewWidget;
var Info = BaseView.extend({
    
    overview_items: function() {
    	
    	var result = [

    		{
    			key: 'state',
    			label: DT.text('state_label'),
    			value: function(state) {
    				return '<span class="' + formatters.statusClassFormatter(state) + '">' + state + '</span>';
    			}
    		}

    	];

    	if (this.model.get('state') === 'RUNNING') {
    		result.push(
    			{ key: 'as_of', label: DT.text('as_of_label') },
    			{ key: 'currentWindowId', label: DT.text('current_wid_label'), title: DT.text('current_wid_title') },
		    	{ key: 'recoveryWindowId', label: DT.text('recovery_wid_label'), title: DT.text('recovery_wid_title') },
		    	{ key: 'stats', label: DT.text('processed_per_sec'), value: function(stats) { return stats.tuplesProcessedPSMA_f || '-' } },
		    	{ key: 'stats', label: DT.text('emitted_per_sec'), value: function(stats) { return stats.tuplesEmittedPSMA_f || '-' } },
		    	{ key: 'stats', label: DT.text('processed_total'), value: function(stats) { return stats.totalTuplesProcessed_f || '-' } },
		    	{ key: 'stats', label: DT.text('emitted_total'), value: function(stats) { return stats.totalTuplesEmitted_f || '-' } },
		    	{ key: 'stats', label: DT.text('num_operators_label'), value: function(stats) { return stats.numOperators } },
		    	{ key: 'stats', label: DT.text('planned/alloc. ctnrs'), value: function(stats, attrs) {
		    		//return stats.plannedContainers + ' / ' + stats.allocatedContainers + ' (' +  attrs.totalAllocatedMemory  + ' GB)';
                    return stats.plannedContainers + ' / ' + stats.allocatedContainers;
		    	}},
			    { key: 'stats', label: DT.text('latency_ms_label'), value: function(stats) { return stats.latency } },
                { key: 'stats', label: DT.text('alloc_mem_label'), value: function(stats) {
                    return formatters.byteFormatter(stats.totalMemoryAllocated, 'mb')
                } }
    		);
    	} else {
    		result.push(
				{ key: 'as_of', label: DT.text('ended_at_label') }
    		);
    	}

    	result.push({
    		key: 'up_for',
    		label: DT.text('up_for_label')
    	});

    	return result;
    }
    
});
exports = module.exports = Info