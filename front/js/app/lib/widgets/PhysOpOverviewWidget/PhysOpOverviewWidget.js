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
 * Overview widget for physical operator
 * 
*/
var _ = require('underscore');
var formatters = DT.formatters;
var templates = DT.templates;
var kt = require('knights-templar');
var BaseView = DT.widgets.OverviewWidget;
var PhysOpOverviewWidget = BaseView.extend({
    
    overview_items: [
        {
            key: 'status',
            label: DT.text('status_label'),
            value: function() {
                return '<span class="' + formatters.statusClassFormatter(status) + '">' + status + '</span>';
            }
        },
        {
            key: 'tuplesProcessedPSMA_f',
            label: DT.text('processed_per_sec')
        },
        {
            key: 'totalTuplesProcessed_f',
            label: DT.text('processed_total')
        },
        {
            key: 'tuplesEmittedPSMA_f',
            label: DT.text('emitted_per_sec')
        },
        {
            key: 'totalTuplesEmitted_f',
            label: DT.text('emitted_total')
        },
        {
            key: 'container',
            label: DT.text('container_id_label'),
            value: function(container, json) {
                return templates.container_link({
                    appId: json.appId,
                    containerId: container,
                    containerIdShort: container.replace(/.*_(\d+)$/, '$1')
                });
            }
        },
        {
            key: 'host',
            label: DT.text('host_label')
        },
        {
            key: 'currentWindowId',
            label: DT.text('current_wid_label')
        },
        {
            key: 'recoveryWindowId',
            label: DT.text('recovery_wid_label')
        },
        {
            key: 'latencyMA',
            label: DT.text('latency_ms_label')
        },
        {
            key: 'cpuPercentageMA',
            label: DT.text('cpu_percentage_label'),
            value: function(cpu) {
                return formatters.percentageFormatter(cpu, true);
            }
        }
    ]
    
});
exports = module.exports = PhysOpOverviewWidget;