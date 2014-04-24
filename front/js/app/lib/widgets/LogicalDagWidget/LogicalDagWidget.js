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
var Backbone = require('backbone');
var kt = require('knights-templar');
var bormat = require('bormat');
var BaseView = require('../DagWidget');
var d3 = require('d3');
var MetricModel = require('./MetricModel');
var MetricModelFactory = require('./MetricModelFactory');
var LogicalOperatorCollection = DT.lib.LogicalOperatorCollection;
var Streams = DT.lib.StreamCollection;
var settings = DT.settings;

/**
 * Visualizer widget for the logical DAG of an application.
 */
var LogicalDagWidget = BaseView.extend({

    initialize: function(options) {

        BaseView.prototype.initialize.call(this, options);

        this.operators = options.operators;

        this.metrics = [
            {
                value: 'none',
                label: 'None'
            },
            {
                value: 'tuplesProcessedPSMA',
                label: DT.text('processed_per_sec')
            },
            {
                value: 'tuplesEmittedPSMA',
                label: DT.text('emitted_per_sec')
            },
            {
                value: 'latencyMA',
                label: DT.text('max_latency_label')
            },
            {
                value: 'partitionCount',
                label: DT.text('partitions_label')
            },
            {
                value: 'containerCount',
                label: DT.text('containers_label')
            },
            {
                value: 'cpuMin',
                label: DT.text('cpu_min_label')
            },
            {
                value: 'cpuMax',
                label: DT.text('cpu_max_label')
            },
            {
                value: 'cpuAvg',
                label: DT.text('cpu_avg_label')
            },
            {
                value: 'lastHeartbeat',
                label: DT.text('last_heartbeat_label')
            },
            {
                value: 'currentWindowId',
                label: DT.text('current_wid_title')
            },
            {
                value: 'recoveryWindowId',
                label: DT.text('recovery_wid_title')
            },
            {
                value: 'totalTuplesProcessed',
                label: DT.text('processed_total')
            },
            {
                value: 'totalTuplesEmitted',
                label: DT.text('emitted_total')
            }
        ];

        this.metricIds = _.map(this.metrics, function (metric) {
            return metric.value;
        });

        this.model.loadLogicalPlan({
            success: _.bind(function(data) {
                this.streams = new Streams(this.model.getStreams());

                this.displayGraph(data.toJSON());

                //this.metricModel = new MetricModel(null, { operators: this.operators });
                this.partitionsMetricModel = MetricModelFactory.getMetricModel('partitionCount');
                //this.listenTo(this.partitionsMetricModel, 'change', this.updatePartitions);
                //this.partitionsMetricModel.subscribe();

                var metric1Id = this.metricIds[1];
                this.metricModel = MetricModelFactory.getMetricModel(metric1Id);
                this.$('.metric-select').val(metric1Id);

                var metric2Id = this.metricIds[2];
                this.metricModel2 = MetricModelFactory.getMetricModel(metric2Id);
                this.$('.metric2-select').val(metric2Id);

                if (!this.collection) {
                    this.collection = new LogicalOperatorCollection([], {
                        appId: options.appId,
                        dataSource: options.dataSource
                    });
                    this.collection.fetch();
                    this.collection.subscribe();
                }

                this.listenTo(this.collection, 'update', this.updateMetrics);

            }, this)
        });
    },

    template: kt.make(__dirname+'/LogicalDagWidget.html','_'),

    html: function() {
        return this.template({ metrics: this.metrics });
    },

    /**
     * Creates legend for stream type display styles
     * 
     * @return {void}
     */
    renderLegend: function () {
        var svgParent = this.$('.svg-legend');
        var elem = svgParent.children('g').get(0);
        var legend = d3.select(elem);

        // Create a data array from all dag edge types (in settings)
        // ['NOT ASSIGNED', 'THREAD_LOCAL', 'CONTAINER_LOCAL', 'NODE_LOCAL', 'RACK_LOCAL'];
        var data = _.map( settings.dag.edges, function (displayProperties, locality) {
            // Looks for a 'displayName' key in the properties first,
            // otherwise just makes the key the label.
            var label = displayProperties.displayName ? displayProperties.displayName : locality;
            return {
                label: label,
                dasharray: displayProperties.dasharray
            };
        });

        // Dimensions for location of label and lines
        var baseY = 20;
        var spaceY = 20;
        var lineBaseY = 15;
        var lineBaseX = 160;
        var lineLength = 200;

        // Add the labels to the legend
        legend.selectAll('text')
            .data(data)
            .enter()
            .append('text')
            .attr('y', function (d, i) {
                return baseY + i * spaceY;
            })
            .text(function (d) {
                return d.label;
            });

        // Add the line samples
        var points = [
            {x: lineBaseX},
            {x: lineBaseX + lineLength}
        ];

        legend.selectAll('g .edge')
            .data(data)
            .enter()
            .append('g')
            .classed('edgePath', true)
            .append('path')
            .attr('marker-end', 'url(#arrowhead)')
            .attr('stroke-dasharray', function (d) {
                return d.dasharray;
            })
            .attr('d', function(d, lineIndex) {
                return d3.svg.line()
                    .x(function(d, i) {
                        return d.x;
                    })
                    .y(function(d, i) {
                        return lineBaseY + lineIndex * spaceY;
                    })
                    (points);
            });;
    },

    /**
     * Creates a graph object that is compatible for dagre-d3 usage.
     * 
     * @param  {Object} data JSON-serialized POJO of logical plan
     * @return {Object}      Transformed object, compatible with dagre-d3.
     */
    buildGraph: function(data) {
        var nodes = [];

        _.each(data.operators, function(value, key) {
            var node = { id: value.name, value: { label: value.name } };
            nodes.push(node);
        });

        var links = [];

        _.each(data.streams, function(stream, key) {
            var source = stream.source.operatorName;
            _.each(stream.sinks, function(sink) {
                var target = sink.operatorName;
                var link = { u: source, v: target, value: { label: stream.name } };
                links.push(link);
            });
        });

        var graph = { nodes: nodes, links: links };
        return graph;
    },

    /**
     * Adds the labels for metrics above and below each logical operator.
     * @param  {dagre.Digraph} graph The graph object for the DAG.
     * @param  {SVGElement}    d3 selection
     * @return {void}
     */
    postRender: function (graph, root) {
        // add metric label, structure is the following
        // g.node
        // g.node-metric-label
        //   text
        //     tspan

        this.graph = graph;
        this.svgRoot = root;
        this.svgNodes = root.selectAll("g .node");

        var that = this;

        this.svgNodes.each(function (d, i) {
            var nodeSvg = d3.select(this);
            var height = graph.node(d).height;

            that.addMetricLabel(nodeSvg, height);
            that.addMetricLabelDown(nodeSvg, height);
        });

        //this.updateStreams(graph, root);
    },

    changeMetric: function () {
        var selMetric = this.$('.metric-select').val();

        this.metricModel = MetricModelFactory.getMetricModel(selMetric);
        this.metricModel.update(this.collection);
        this.updateMetricLabels(this.metricModel);
    },

    changeMetric2: function () {
        var selMetric = this.$('.metric2-select').val();

        this.metricModel2 = MetricModelFactory.getMetricModel(selMetric);
        this.metricModel2.update(this.collection);
        this.updateMetric2Labels(this.metricModel2);
    }

});

exports = module.exports = LogicalDagWidget