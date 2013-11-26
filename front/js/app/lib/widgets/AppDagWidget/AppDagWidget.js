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
 * Info widget for port instances.
 */
var _ = require('underscore');
var kt = require('knights-templar');
var bormat = require('bormat');
var BaseView = DT.widgets.Widget;
var d3 = require('d3');
var dagreD3 = require('dagre-d3');

var AppDagWidget = BaseView.extend({
    initialize: function(options) {
        BaseView.prototype.initialize.call(this, options);
        if (options.physicalPlan) {
            this.model.loadPhysicalPlan({
                success: _.bind(function(data) {
                    this.displayGraph(data.toJSON(), true);
                }, this)
            });
        } else {
            this.model.loadLogicalPlan({
                success: _.bind(function(data) {
                    this.displayGraph(data.toJSON());
                }, this)
            });
        }
    },

    template: kt.make(__dirname+'/AppDagWidget.html','_'),

    html: function() {
        return this.template();
    },

    displayGraph: function(data, physicalPlan) {
        if (physicalPlan) {
            var graph = this.buildPhysicalGraph(data);
            this.renderPhysicalGraph(graph, this.$el.find('.app-dag > svg')[0]);
        } else {
            var graph = this.buildGraph(data);
            this.renderGraph(graph, this.$el.find('.app-dag > svg')[0]);
        }
    },

    buildPhysicalGraph: function(data) {
        var nodes = [];
        var containers = {};
        var containerCount = 0;
        var nodeMap = {};

        _.each(data.operators, function(value, key) {
            var containerId = value.container;
            if (containers.hasOwnProperty(containerId)) {
                containerIndex = containers[containerId];
            } else {
                containerIndex = containerCount++;
                containers[containerId] = containerIndex;
            }

            var containerShortId = parseInt(containerId.substring(containerId.lastIndexOf('_') + 1));

            //var label = value.name + ' (' + value.id + ')';
            var label = value.name + ' (' + containerShortId + ')';
            var node = { id: value.id, value: { label: label }, containerIndex: containerIndex, data: value };
            nodes.push(node);
            nodeMap[node.id] = node;
        });

        var links = [];

        _.each(data.streams, function(stream, key) {
            var source = stream.source.operatorId;
            _.each(stream.sinks, function(sink) {
                var target = sink.operatorId;
                //var link = { u: source, v: target };
                var link = { u: source, v: target, value: { label: stream.logicalName } };
                links.push(link);
            });
        });

        var graph = { nodes: nodes, links: links, nodeMap: nodeMap };
        return graph;
    },

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
        var layout = dagreD3.layout().rankDir('LR');
        renderer.layout(layout).run(dagreD3.json.decode(nodes, links), svg.append("g"));

        // TODO
        // Adjusting height to content
        var main = svgParent.find('g > g');
        var h = main.get(0).getBoundingClientRect().height;
        var newHeight = h + 40;
        newHeight = newHeight < 80 ? 80 : newHeight;
        newHeight = newHeight > 500 ? 500 : newHeight;
        svgParent.height(newHeight);

        // Zoom
        d3.select(svgParent.get(0)).call(d3.behavior.zoom().on("zoom", function() {
            var ev = d3.event;
            svg.select("g")
                .attr("transform", "translate(" + ev.translate + ") scale(" + ev.scale + ")");
        }));
    },

    renderPhysicalGraph: function(graph, selector) {
        var svgParent = jQuery(selector);
        var nodes = graph.nodes;
        var links = graph.links;
        var nodeMap = graph.nodeMap;

        var graphElem = svgParent.children('g').get(0);
        var svg = d3.select(graphElem);
        svg.selectAll("*").remove();

        var renderer = new dagreD3.Renderer();
        var layout = dagreD3.layout().rankDir('LR');
        renderer.layout(layout).run(dagreD3.json.decode(nodes, links), svg.append("g"));

        var colors = d3.scale.category20(); //TODO do not limit to 20 colors

        d3.select(selector).selectAll('.node > g > rect').style('stroke', function(key, index) {
            var node = nodeMap[key];
            var color = colors(node.containerIndex % 20);
            return color;
        });

        /*
        d3.select(selector).selectAll('.node > g > g > text').style('fill', function(key, index) {
            var node = nodeMap[key];
            var color = colors(node.containerIndex % 20);
            return color;
        });
        */

        // TODO
        // Adjusting height to content
        var main = svgParent.find('g > g');
        var h = main.get(0).getBoundingClientRect().height;
        var newHeight = h + 40;
        newHeight = newHeight < 80 ? 80 : newHeight;
        newHeight = newHeight > 1000 ? 1000 : newHeight;
        svgParent.height(newHeight);

        // Zoom
        d3.select(svgParent.get(0)).call(d3.behavior.zoom().on("zoom", function() {
            var ev = d3.event;
            svg.select("g")
                .attr("transform", "translate(" + ev.translate + ") scale(" + ev.scale + ")");
        }));
    }

});

exports = module.exports = AppDagWidget