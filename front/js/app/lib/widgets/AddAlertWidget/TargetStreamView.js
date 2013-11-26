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
 * TargetStreamView
 * 
 * View that encapsulates the select dropdown 
 * where the user chooses which stream to listen
 * to for an alert condition.
*/
var _ = require('underscore');
var kt = require('knights-templar');
var Bbind = DT.lib.Bbindings;
var bassview = require('bassview');
var TargetStreamView = bassview.extend({
    
    initialize: function(options) {
        // this.model : the alert model
        this.instance = options.instance;
        this.dataSource = options.dataSource;
        
        // select subview
        this.subview('select', new Bbind.select({
            model: this.model,
            attr: 'streamName',
            classElement: function($el) {
                return $el.parent();
            },
            errorClass: 'error'
        }));
        
        // evt listeners
        this.listenTo(this.model, 'change:operatorName', this.render);
    },
    
    render: function() {
        var json = {
            streamOptions: this.getStreamOptions(),
            alert: this.model.toJSON(),
            instance: this.instance.toJSON()
        }
        var html = this.template(json);
        this.$el.html(html);
        this.assign('#streamName', 'select');
        return this;
    },
    
    // Creates a list of possible streams to set the alert on.
    // If a port on the chosen operator does not have a stream
    // attached to it, the stream name should be <operatorName>.<portName>
    getStreamOptions: function(noRender) {
        var operatorName = this.model.get('operatorName');
        var streams = this.instance.getStreams();
        var disconnectedPorts = [];
        var logicalPlan = this.instance.get('logicalPlan').toJSON();
        if (_.isEmpty(logicalPlan)) {
            return {
                streams: [],
                ports: []
            };
        }
        var operator = logicalPlan.operators[operatorName];
            
        if (!operatorName) {
            return false;
        }   
        
        if (!operator) {
            return false;
        }
        
        // tracks any output ports not connected to a stream
        var portCheckList = {};
        
        // get the output ports on the operator
        var outputPorts = _.chain(operator.ports)
            // add portName to each object
            .map(function(val,key) {
                val['portName'] = key;
                return val;
            })
            // filter out input ports
            .filter(function(obj) {
                return obj.type === 'output';
            })
            // populate the portCheckList
            .each(function(obj) {
                portCheckList[obj['portName']] = false;
            })
            .value();
        
        
        // Filter streams so that only those that have 
        // a source of one of the output ports is there.
        streams = _.filter(streams, function(stream) {

            if (stream.source.operatorName !== operatorName) return false;
            
            var result = portCheckList.hasOwnProperty(stream.source.portName);

            if (result) {
                portCheckList[stream.source] = true;
            }
            
            return result;
        });
        
        streams = _.map(streams, function(stream) {
            return stream.name;
        });
        
        // Look at the checklist for disconnected ports
        _.each(portCheckList, function(isConnected, portName) {
            if (!isConnected) {
                disconnectedPorts.push(operatorName + '.' + portName);
            }
        });
        
        return {
            streams: streams,
            ports: disconnectedPorts
        };
    },
    
    template: kt.make(__dirname+'/TargetStreamView.html','_')
    
});
exports = module.exports = TargetStreamView