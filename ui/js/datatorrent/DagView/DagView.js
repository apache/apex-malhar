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
 * Dag View
 * 
 * 
 * Base view class for visualizing a DAG.
 * Uses jsPlumb
*/
var _ = require('underscore');
var bassview = require('bassview');
var OperatorClassModel = require('../OperatorClassModel');
var OperatorClassCollection = require('../OperatorClassCollection');
var DagOperatorView = require('../DagOperatorView');
var DagView = bassview.extend({
    
    initialize: function(options) {
        
        // TODO: shim jsPlumb so its not global
        this.plumb = jsPlumb.getInstance({
            // Container: this.el,
            Connector: 'Flowchart',
            // default drag options
			DragOptions : { cursor: 'pointer', zIndex:2000 },
			// default to blue at one end and green at the other
			EndpointStyles : [{ fillStyle:'#225588' }, { fillStyle:'#558822' }],
			// blue endpoints 7 px; green endpoints 11.
			Endpoints : [ [ "Dot", {radius:7} ], [ "Dot", { radius:11 } ]],
			ConnectionsDetachable:true,
			// the overlays to decorate each connection with.  note that the label overlay uses a function to generate the label text; in this
			// case it returns the 'labelText' member that we set on each connection in the 'init' method below.
			ConnectionOverlays : [
                // [ "Arrow", { location:1, paintStyle: { strokeStyle: 'rgb(100,0,0)', fillStyle: 'rgb(100,0,0)'} } ],
				[ "Label", { 
					location:0.5,
					id:"label",
					cssClass:"aLabel"
				}]
			]
        });
        
        // Set up operator class collection
        this.operators = new OperatorClassCollection([]);
        
        // Set up connections object
        this.connections = {};
        
        // Set up hash for tag_id->operator
        this._operator_id_lookup = {};
        
        // Listen for connections
        this.plumb.bind('connection', this._onConnection.bind(this));
        this.plumb.bind('connectionDetached', this._onConnectionDetach.bind(this));
        
    },
    
    // Adds the operator to the element, with an initial position (x,y)
    // x can be a number or a string: "middle"|"left"|"right".
    // y cna be a number or a string: "middle"|"top"|"bottom".
    addOperator: function(operator, options) {
        
        _.defaults(options, {
            label: '',
            x: 'middle',
            y: 'middle',
            draggable: true
        });
        
        if ( !(operator instanceof OperatorClassModel) ) {
            throw new TypeError('addOperator requires an instance of OperatorClassModel');
        }
        
        // create new dag op view
        var view = new DagOperatorView({
            model: operator,
            label: options.label,
            plumb: this.plumb
        });
        this.subview('operator' + this.operators.length, view);
        
        // set x and y of its $el
        x = this.getXValue(options.x);
        y = this.getYValue(options.y);
        view.$el.css({
            'left': x,
            'top': y,
            'width': this.operatorDimensions[0],
            'height': this.operatorDimensions[1]
        });
        
        // append to this.$el
        this.$el.append(view.render().$el);
        
        this.$el.attr('id', this.plumb.getId(this.el));
        
        // make draggable
        if (options.draggable) {
            this.plumb.draggable(view.$el, { containment: this.$el });
        }
        
        // get the generated id?
        this._operator_id_lookup[view.$el.attr('id')] = operator;
        
        // make endpoints?
        this.operators.add(operator);
    },
    
    getXValue: function(x) {
        if (typeof x === 'number') {
            return x;
        }
        
        var width = this.$el.width() || this.containerWidth;
        
        var opWidth = this.operatorDimensions[0];
        
        switch(x) {
            case "middle":
                return (width - opWidth) / 2;
            
            case "left":
                return this.containerPadding['left'];
                
            case "right":
                return width - opWidth - this.containerPadding['right'];
            
            default:
                return 0;
        }
        
    },
    
    getYValue: function(y) {
        if (typeof y === 'number') {
            return y;
        }
        
        var height = this.$el.height() || this.containerHeight;
        var opHeight = this.operatorDimensions[1];
        
        switch(y) {
            case "top":
                return this.containerPadding['top'];
            
            case "middle":
                return (height - opHeight) / 2;
            
            case "bottom":
                return height - opHeight - this.containerPadding['bottom'];
            
            default:
                return 0;
        }
    },
    
    clearAllOperators: function() {
        this.trigger('clean_up');
        this.operators.reset([]);
    },
    
    _onConnection: function(info) {
        var connection = info.connection;
        var sEP = info.sourceEndpoint;
        var tEP = info.targetEndpoint;
        
        this.connections[connection.id] = {
            'source': {
                'operator': this._operator_id_lookup[info.sourceId],
                'port': sEP.canvas.title
            },
            'target': {
                'operator': this._operator_id_lookup[info.targetId],
                'port': tEP.canvas.title
            }
        }
        
        this.trigger('connection', this.connections[connection.id]);
    },
    
    _onConnectionDetach: function(info) {
        var connection_id = info.connection.id;
        var connection = this.connections[connection_id];
        delete this.connections[connection_id];
        
        this.trigger('disconnection', connection);
    },
    
    remove: function() {
        this.plumb.unbind();
        this.plumb.unmakeEverySource();
        this.plumb.unmakeEveryTarget();
        this.plumb.reset();
        delete window.jsPlumbInstance;
        delete this.plumb;
        delete this.connections;
        delete this._operator_id_lookup;
        bassview.prototype.remove.call(this);
    },
    
    operatorDimensions: [75, 75],
    
    containerPadding: {
        top: 5,
        right: 5,
        bottom: 5,
        left: 5
    },
    
    containerWidth: 200
    
});
exports = module.exports = DagView;