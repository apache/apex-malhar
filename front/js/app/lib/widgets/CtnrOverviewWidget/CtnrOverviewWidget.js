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
var kt = require('knights-templar');
var BaseView = DT.widgets.OverviewWidget;

/**
 * Overview widget for container
 * 
*/
var CtnrOverviewWidget = BaseView.extend({

    overview_items: [
    	{
    		label: DT.text('state_label'),
    		key: 'state',
    		value: function(state) {
    			return '<span class="' + DT.formatters.statusClassFormatter(state) + '">' + state + '</state>';
    		}
    	},
    	{
    		label: DT.text('alloc_mem_mb_label'),
    		key: 'memoryMBAllocated'
    	},
    	{
    		label: DT.text('free_mem_mb_label'),
    		key: 'memoryMBFree'
    	},
    	{
    		label: DT.text('num_operators_label'),
    		key: 'numOperators'
    	},
    	{
    		label: DT.text('as_of_label'),
    		key: 'as_of'
    	},
    	{
    		label: DT.text('current_wid_label'),
    		title: DT.text('current_wid_title'),
    		key: 'currentWindowId'
    	},
    	{
    		label: DT.text('recovery_wid_label'),
    		title: DT.text('recovery_wid_title'),
    		key: 'recoveryWindowId'
    	},
    	{
    		label: DT.text('processed_per_sec'),
    		key: 'tuplesProcessedPSMA_f'
    	},
    	{
    		label: DT.text('emitted_per_sec'),
    		key: 'tuplesEmittedPSMA_f'
    	}
    ]
    
});
exports = module.exports = CtnrOverviewWidget;