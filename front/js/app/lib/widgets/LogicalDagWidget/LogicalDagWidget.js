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
var BaseView = DT.widgets.Widget;
var d3 = require('d3');
var dagreD3 = require('dagre-d3');
var MetricModel = require('./MetricModel');
var MetricModelFactory = require('./MetricModelFactory');
var LogicalOperatorCollection = DT.lib.LogicalOperatorCollection;
var Streams = DT.lib.StreamCollection;
var settings = DT.settings;

/**
 * Visualizer widget for the logical DAG of an application.
 */
var LogicalDagWidget = BaseView.extend({

    events: {
        'change .metric-select': 'changeMetric',
        'click .metric-prev': 'prevMetric',
        'click .metric-next': 'nextMetric',
        'change .metric2-select': 'changeMetric2',
        'click .metric-prev2': 'prevMetric2',
        'click .metric-next2': 'nextMetric2',
        'click .toggle-locality': 'toggleLocality',
        'click .toggle-legend': 'toggleLegend'
    },

    showLocality: false,

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
                label: 'Max Latency (ms)'
            },
            {
                value: 'partitionCount',
                label: 'Partition Count'
            },
            {
                value: 'containerCount',
                label: 'Container Count'
            },
            {
                value: 'cpuPercentageMA',
                label: 'CPU (%)'
            },
            {
                value: 'lastHeartbeat',
                label: 'Last Heartbeat'
            },
            {
                value: 'currentWindowId',
                label: 'Current Window'
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
     * Renders legend, renders graph to .svg-main element
     * 
     * @param  {Object} data       JSON-serialized POJO of logical plan
     * @return {void}
     */
    displayGraph: function(data) {
        this.renderLegend();
        var graph = this.buildGraph(data);
        this.renderGraph(graph, this.$('.app-dag > .svg-main')[0]);
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

    renderGraph: function(graph, selector) {
        var svgParent = jQuery(selector);
        var nodes = graph.nodes;
        var links = graph.links;

        var graphElem = svgParent.children('g').get(0);
        var svg = d3.select(graphElem);
        svg.selectAll("*").remove();

        var renderer = new dagreD3.Renderer();

        var oldPostRender = renderer._postRender;
        renderer._postRender = function (graph, root) {
            oldPostRender.call(renderer, graph, root);
            this.postRender(graph, root);
        }.bind(this);

        renderer._calculateEdgeDimensions = function (group, value) {
            var bbox = group.getBBox();
            value.width = bbox.width + 10;
            value.height = bbox.height;
        };

        var layout = dagreD3.layout().rankDir('LR');
        renderer.layout(layout).run(dagreD3.json.decode(nodes, links), svg.append("g"));

        // TODO
        // Adjusting height to content
        var main = svgParent.find('g > g');
        var h = main.get(0).getBoundingClientRect().height;
        var newHeight = h + 50;
        newHeight = newHeight < 110 ? 110 : newHeight;
        newHeight = newHeight > 500 ? 500 : newHeight;
        svgParent.height(newHeight);

        // Zoom
        d3.select(svgParent.get(0)).call(d3.behavior.zoom().on("zoom", function() {
            var ev = d3.event;
            svg.select("g")
                .attr("transform", "translate(" + ev.translate + ") scale(" + ev.scale + ")");
        }));
    },

    toggleLocality: function (event) {
        event.preventDefault();

        var toggleLocalityLink = this.$el.find('.toggle-locality');
        var legend = this.$el.find('.logical-dag-legend');

        this.showLocality = !this.showLocality;

        if (this.showLocality) {
            toggleLocalityLink.text('Hide Stream Locality');
            this.updateStreams(this.graph, this.svgRoot);
            legend.show();
        } else {
            toggleLocalityLink.text('Show Stream Locality');
            this.clearStreamLocality(this.svgRoot);
            legend.hide();
        }
    },

    toggleLegend: function (event) {
        event.preventDefault();

        var toggleLink = this.$el.find('.toggle-legend');
        var legend = this.$el.find('.logical-dag-legend');

        if (legend.is(':visible')) {
            toggleLink.text('Show Legend');
            legend.hide();
        } else {
            toggleLink.text('Hide Legend');
            legend.show();
        }
    },

    changeMetric: function () {
        var selMetric = this.$('.metric-select').val();

        this.metricModel = MetricModelFactory.getMetricModel(selMetric);
        this.metricModel.update(this.collection);
        this.updateMetricLabels(this.metricModel);
    },

    prevMetric: function (event) {
        event.preventDefault();
        var selMetric = this.$('.metric-select').val();
        var index = _.indexOf(this.metricIds, selMetric);
        var nextIndex = (this.metricIds.length + index - 1) % this.metricIds.length;
        this.$('.metric-select').val(this.metricIds[nextIndex]);
        this.changeMetric();
    },

    nextMetric: function (event) {
        event.preventDefault();
        var selMetric = this.$('.metric-select').val();
        var index = _.indexOf(this.metricIds, selMetric);
        var nextIndex = (index + 1) % this.metricIds.length;
        this.$('.metric-select').val(this.metricIds[nextIndex]);
        this.changeMetric();
    },

    changeMetric2: function () {
        var selMetric = this.$('.metric2-select').val();

        this.metricModel2 = MetricModelFactory.getMetricModel(selMetric);
        this.metricModel2.update(this.collection);
        this.updateMetric2Labels(this.metricModel2);
    },

    prevMetric2: function (event) {
        event.preventDefault();
        var selMetric = this.$('.metric2-select').val();
        var index = _.indexOf(this.metricIds, selMetric);
        var nextIndex = (this.metricIds.length + index - 1) % this.metricIds.length;
        this.$('.metric2-select').val(this.metricIds[nextIndex]);
        this.changeMetric2();
    },

    nextMetric2: function (event) {
        event.preventDefault();
        var selMetric = this.$('.metric2-select').val();
        var index = _.indexOf(this.metricIds, selMetric);
        var nextIndex = (index + 1) % this.metricIds.length;
        this.$('.metric2-select').val(this.metricIds[nextIndex]);
        this.changeMetric2();
    },

    updateMetrics: function () {
        var changed = this.partitionsMetricModel.update(this.collection, true);

        if (changed) {
            this.updatePartitions();
        }

        if (!this.metricModel.isNone()) {
            this.metricModel.update(this.collection);
            this.updateMetricLabels(this.metricModel);
        }

        if (!this.metricModel2.isNone()) {
            this.metricModel2.update(this.collection);
            this.updateMetric2Labels(this.metricModel2);
        }
    },

    updatePartitions: function () {
        var that = this;
        this.svgNodes.each(function (d, i) {
            var nodeSvg = d3.select(this);

            var multiple = that.partitionsMetricModel.showMetric(d);

            var filter = multiple ? 'url(#f1)' : null;
            //var nodeLabel = nodeSvg.select('.label');
            //nodeLabel.attr('filter', filter);

            var nodeRect = nodeSvg.select('.label > rect');
            nodeRect.attr('filter', filter);
        });
    },

    updateMetricLabels: function (metric) {
        var that = this;
        var graph = this.graph;
        this.svgNodes.each(function (d, i) {
            var nodeSvg = d3.select(this);
            that.updateMetricLabel(graph, metric, d, nodeSvg);
        });
    },

    updateMetric2Labels: function (metric) {
        var that = this;
        var graph = this.graph;
        this.svgNodes.each(function (d, i) {
            var nodeSvg = d3.select(this);
            that.updateMetric2Label(graph, metric, d, nodeSvg);
        });
    },


    updateMetricLabel: function (graph, metric, d, nodeSvg) {
        var value = metric.getTextValue(d);
        var showMetric = metric.showMetric(d);

        var metricLabel = nodeSvg.select('.node-metric-label');
        var metricLabelText = metricLabel.select('tspan');

        var text = showMetric ? value : '';
        metricLabelText.text(text);

        var bbox = metricLabel.node().getBBox();
        var height = graph.node(d).height;
        metricLabel.attr("transform",
            "translate(" + (-bbox.width / 2) + "," + (-bbox.height - height / 2 - 4) + ")");
    },

    updateMetric2Label: function (graph, metric, d, nodeSvg) {
        var value = metric.getTextValue(d);
        var showMetric = metric.showMetric(d);

        var metricLabel = nodeSvg.select('.node-metric2-label');
        var metricLabelText = metricLabel.select('tspan');

        var text = showMetric ? value : '';
        metricLabelText.text(text);

        var bbox = metricLabel.node().getBBox();
        var height = graph.node(d).height;

        metricLabel.attr("transform",
            "translate(" + (-bbox.width / 2) + "," + (-bbox.height + height + 4) + ")");
    },

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

    createStreamLocalityMap: function () {
        var streamLocality = {};
        this.streams.each(function (stream) {
            if (stream.has('locality')) {
                streamLocality[stream.get('name')] = stream.get('locality');
            }
        });

        return streamLocality;
    },

    clearStreamLocality: function (root) {
        root.selectAll("g .edge > path").attr('stroke-dasharray', null);
    },

    updateStreams: function (graph, root) {
        var streamLocality = this.createStreamLocalityMap();

        root.selectAll("g .edge > path").each(function (d) {
            var value = graph.edge(d);
            var streamName = value.label;

            var locality = streamLocality.hasOwnProperty(streamName) ? streamLocality[streamName] : 'NONE';
            var localityDisplayProperty = settings.dag.edges.hasOwnProperty(locality) ? settings.dag.edges[locality] : settings.dag.edges.NONE;

            if (localityDisplayProperty.dasharray) {
                d3.select(this).attr('stroke-dasharray', localityDisplayProperty.dasharray);
            }
        });
    },

    addMetricLabel: function (nodeSvg, height) {
        var labelSvg = nodeSvg.append("g").attr('class', 'node-metric-label');
        labelSvg
            .append("text")
            .attr("text-anchor", "left")
            .append("tspan")
            .attr("dy", "1em");

        var bbox = labelSvg.node().getBBox();

        labelSvg.attr("transform",
            "translate(" + (-bbox.width / 2) + "," + (-bbox.height - height / 2 - 4) + ")");
    },

    addMetricLabelDown: function (nodeSvg, height) {
        var labelSvg = nodeSvg.append("g").attr('class', 'node-metric2-label');
        labelSvg
            .append("text")
            .attr("text-anchor", "left")
            .append("tspan")
            .attr("dy", "1em");

        var bbox = labelSvg.node().getBBox();

        labelSvg.attr("transform",
            "translate(" + (-bbox.width / 2) + "," + (-bbox.height + height + 4) + ")");
    },

    renderLegend: function () {
        var svgParent = this.$el.find('.svg-legend');
        var elem = svgParent.children('g').get(0);
        var svg = d3.select(elem);

        //var data = ['NOT ASSIGNED', 'THREAD_LOCAL', 'CONTAINER_LOCAL', 'NODE_LOCAL', 'RACK_LOCAL'];
        var data = _.map(_.keys(settings.dag.edges), function (locality) {
            var displayProperties = settings.dag.edges[locality];
            var label = displayProperties.displayName ? displayProperties.displayName : locality;
            return {
                label: label,
                dasharray: displayProperties.dasharray
            };
        });

        var baseY = 20;
        var spaceY = 20;
        var lineBaseY = 15;
        var lineBaseX = 160;
        var lineLength = 200;

        svg.selectAll('text')
            .data(data)
            .enter()
            .append('text')
            .attr('y', function (d, i) {
                return baseY + i * spaceY;
            })
            .text(function (d) {
                return d.label;
            });

        var points = [
            {x: lineBaseX},
            {x: lineBaseX + lineLength}
        ];

        svg.selectAll('g .edge')
            .data(data)
            .enter()
            .append('g')
            .classed('edge', true)
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
    }

});

exports = module.exports = LogicalDagWidget