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
var d3 = require('d3');
var kt = require('knights-templar');
var BaseView = require('../DagWidget');

/**
 * Physical DAG Widget
*/
var PhysicalDagWidget = BaseView.extend({
    
    initialize: function(options) {
        
        BaseView.prototype.initialize.call(this, options);

        this.model.loadPhysicalPlan({
            success: _.bind(function(data) {
                this.displayGraph(data.toJSON());
            }, this)
        });
        
    },
    
    template: kt.make(__dirname+'/PhysicalDagWidget.html','_'),

    html: function() {
        return this.template({});
    },

    buildGraph: function(data) {
        var nodes = [];
        var containers = {};
        var containerCount = 0;
        var nodeMap = this.nodeMap = {};

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
            //var label = value.name + ' (' + containerShortId + ')';
            var label = value.name;
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

    postRender: function(graph, root) {
        var nodeMap = this.nodeMap;
        var colors = d3.scale.category20(); //TODO do not limit to 20 colors

        root.selectAll('.node > g > rect').style('stroke', function(key, index) {
            var node = nodeMap[key];
            var color = colors(node.containerIndex % 20);
            return color;
        });
    }
    
});

exports = module.exports = PhysicalDagWidget;