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
 * Stream Model
*/
var _ = require('underscore');
var Backbone = require('backbone');
var PortCollection = require('./StreamPortCollection');

var StreamModel = Backbone.Model.extend({
    
    defaults: {
        'name': '',
        'appId': '',
        'source': {},
        'sinks': []
    },
    
    idAttribute: 'name',
    
    initialize: function(attrs, options) {
        
        // Check if instance is provided
        if (options.application) {
            this.application = options.application;
            this.listenTo(options.application, 'change:logicalPlan', function() {
                var stream = this.application.getStream(this.get('name'));
                this.set(stream);
            });
        }
        
    },
    
    setSourcePorts: function(ports) {
        return this._setPorts(ports, 'Source');
    },
    
    setSinkPorts: function(ports) {
        return this._setPorts(ports, 'Sink');
    },
    
    _setPorts: function(ports, PortType) {

        var fn_name = 'set' + PortType + 'Ports',
            attr_name = PortType.toLowerCase() + 'Ports';

        if (!this.application) {
            throw new Error(fn_name + ' requires presence of application');
        }

        if (!this[attr_name]) {
            this[attr_name] = new PortCollection([], {
                appId: this.application.get('id')
            });
            this.listenTo(this[attr_name], 'update', this.updatePortAggValues);
        }

        this[attr_name].set(ports);

        return this;
    },
    
    updatePortAggValues: function() {
        
    },
    
    subscribeToUpdates: function() {
        if (!this.application) {
            throw new Error('StreamModel needs the application instance model to subscribeToUpdates');
        }

        this.listenTo(this.application.operators, 'update', this.updatePorts);
        return this;
    },
    
    updatePorts: function() {
        
        var source = this.get('source');
        var sinks = this.get('sinks');
        var source_op_name = source.operatorName;
        var source_port_name = source.portName;
        var sink_op_names = _.map(sinks, function(sink) { return sink.operatorName; });
        var sink_port_names = _.map(sinks, function(sink) { return sink.portName; });
        
        var source_ops = [],
            sink_ops = [],
            source_ports,
            sink_ports;
        
        this.application.operators.each(function(op) {
            var opName = op.get('name');

            if ( opName === source_op_name) {
                source_ops.push(op);
                return;
            }
            
            if ( sink_op_names.indexOf(opName) > -1 ) {
                sink_ops.push(op);
                return;
            }
            
        });
        
        source_ports = _.reduce(source_ops, function(memo, op) {
            var newPorts = op.get('ports');
            var filteredPorts = _.filter(newPorts, function(port) {
            port.operator = op;
                return port.name === source_port_name;
            });
            memo = memo.concat(filteredPorts);
            return memo;
        }, []);
        
        sink_ports = _.reduce(sink_ops, function(memo, op) {
            var newPorts = op.get('ports');
            var filteredPorts = _.filter(newPorts, function(port) {
                port.operator = op;
                return sink_port_names.indexOf(port.name) > -1;
            });
            memo = memo.concat(filteredPorts);
            return memo;
        }, []);
        
        this.setSourcePorts(source_ports);
        
        this.setSinkPorts(sink_ports);
        
        return this;
        
    },
    
    unsetPortCollections: function() {
        _.each(['sourcePorts', 'sinkPorts'], function(key) {
            if (this[key]) {
                this[key].stopListening();
                this.stopListening(this[key]);
                this[key] = undefined;
                delete this[key];
            }
        }, this);
    },
    
    cleanUp: function() {
        this.stopListening();
        this.unsetPortCollections();
    }
    
});

exports = module.exports = StreamModel;