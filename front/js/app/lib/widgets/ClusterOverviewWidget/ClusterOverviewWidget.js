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
var formatters = DT.formatters;
var kt = require('knights-templar');
var BaseView = DT.widgets.OverviewWidget;
var Notifier = DT.lib.Notifier;
var ClusterMetricsModel = DT.lib.ClusterMetricsModel;

/**
 * This widget displays info about the cluster.
 * 
*/
var ClusterOverviewWidget = BaseView.extend({
    
    initialize: function(options) {
        
        // Set up cluster model
        this.model = new ClusterMetricsModel({}, { dataSource: options.dataSource });
        this.model.fetch();
        this.model.subscribe();

        // super
        BaseView.prototype.initialize.call(this, options);
    },

    events: {
        'click .refreshCluster': 'refreshCluster'
    },

    refreshCluster: function() {
        this.model.fetch({
            success: function() {
                Notifier.success({
                    title: DT.text('Cluster Info Refreshed'),
                    text: DT.text('The cluster info has been successfully updated.')
                });
            },
            error: function(xhr, textStatus, errorThrown) {
				Notifier.success({
					title: DT.text('Error Refreshing Cluster'),
					text: DT.text('An error occurred refreshing the cluster. Error: ') + errorThrown
				});
            }
        });
    },

    remove: function() {
        this.model.unsubscribe();
        BaseView.prototype.remove.call(this);
    },

    overview_items: [

        {
            label: DT.text('cores_label'),
            key: 'cpuPercentage',
            value: function(cpuPercentage) {
                if (!cpuPercentage) {
                    return '-';
                }
                return formatters.cpusFormatter(cpuPercentage, true);
            }
        },

        {
            label: DT.text('current alloc mem'),
            key: 'currentMemoryAllocatedMB',
            value: function(currentMemoryAllocatedMB, attrs) {
                return formatters.byteFormatter(currentMemoryAllocatedMB, 'mb');
            }
        },

        {
            label: DT.text('peak alloc mem'),
            key: 'maxMemoryAllocatedMB',
            value: function(maxMemoryAllocatedMB) {
                return formatters.byteFormatter(maxMemoryAllocatedMB, 'mb');
            }
        },

        {
            label: DT.text('running / pending / failed / finished / killed / submitted'),
            key: 'numAppsRunning',
            value: function(numAppsRunning, attrs) {
                return '<span class="status-running">' + formatters.commaGroups(attrs.numAppsRunning) + '</span> / ' +
                       '<span class="status-pending-deploy">' + formatters.commaGroups(attrs.numAppsPending) + '</span> / ' +
                       '<span class="status-failed">' + formatters.commaGroups(attrs.numAppsFailed) + '</span> / ' +
                       '<span class="status-finished">' + formatters.commaGroups(attrs.numAppsFinished) + '</span> / ' +
                       '<span class="status-killed">' + formatters.commaGroups(attrs.numAppsKilled) + '</span> / ' +
                       '<span class="status-submitted">' + formatters.commaGroups(attrs.numAppsSubmitted) + '</span>';
            }
        },
        
        {
            label: DT.text('num_containers_label'),
            key: 'numContainers'
        },

        {
            label: DT.text('num_operators_label'),
            key: 'numOperators'
        },
        
        {
            label: DT.text('tuples_per_sec'),
            key: 'tuplesProcessedPSMA',
            value: function(val) {
                return formatters.commaGroups(val);
            }
        },
        
        {
            label: DT.text('emitted_per_sec'),
            key: 'tuplesEmittedPSMA',
            value: function(val) {
                return formatters.commaGroups(val);
            }
        }

    ]
    
});

exports = module.exports = ClusterOverviewWidget;