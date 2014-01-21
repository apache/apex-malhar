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
var BaseView = require('../DagWidget');


/**
 * JarDagWidget
*/
var JarDagWidget = BaseView.extend({
    
    initialize: function(options) {

        BaseView.prototype.initialize.call(this, options);

        this.model.loadLogicalPlan({
            success: _.bind(function(data) {
                this.displayGraph(data.toJSON());
            }, this)
        });
    },

    template: kt.make(__dirname+'/JarDagWidget.html','_'),

    html: function() {
        return this.template({});
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
    }
    
});

exports = module.exports = JarDagWidget;