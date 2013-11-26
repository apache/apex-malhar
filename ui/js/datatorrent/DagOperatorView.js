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
 * DagOperatorSubView
 * 
 * 
 * This view's el is the operator used in a dag view
*/

var _ = require('underscore');
var kt = require('knights-templar');
var bassview = require('bassview');
var connectorPaintStyle = {
	lineWidth:4,
	strokeStyle:'#5C96BC',
	joinstyle:'round',
	outlineColor:'#eaedef',
	outlineWidth:2
},

connectorHoverStyle = {
	lineWidth:4,
	strokeStyle:'#deea18',
	outlineWidth:2,
	outlineColor:'white'
},

endpointHoverStyle = {
    fillStyle:'#5C96BC',
    outlineWidth:0
},
inputEndpointOptions = {
	endpoint:'Dot',
	paintStyle:{
	    strokeStyle:'#1da8db',
		fillStyle:'transparent',
		radius:7,
		lineWidth:2
	},
	hoverPaintStyle:endpointHoverStyle,
	dropOptions:{ hoverClass:'hover', activeClass:'active' },
	isTarget:true,
    overlays:[
        [ 'Label', {
        location:[0.5, -0.5],
            label:'Drop',
            cssClass:'endpointTargetLabel',
            id: 'label'
        } ]
    ]
},
outputEndpointOptions = {
	endpoint:'Dot',
	paintStyle:{
		fillStyle:'#64c539',
		radius:8
	},
	isSource:true,
	connector:[ 'Flowchart', { stub:[40, 60], gap:10, cornerRadius:5, alwaysRespectStubs:true } ],
    connectorStyle: connectorPaintStyle,
	hoverPaintStyle:endpointHoverStyle,
    connectorHoverStyle:connectorHoverStyle,
    dragOptions:{},
    overlays:[
        [ 'Label', {
            location:[0.5, 1.5],
            label:'Drag',
            cssClass:'endpointSourceLabel',
            id: 'label'
        } ]
    ]
};

var DagOperatorView = bassview.extend({

    initialize: function(options) {
        this.plumb = options.plumb;
        this.label = options.label;
        this.listenTo(this.model, 'change:name', this.render);
        this.listenTo(this.model, 'change:inputPorts', this.updateEndpoints);
        // this.listenTo(this.model, 'change', this.updateEndpoints);
    },

    className: 'dag-operator',
    
    inputEndpoints: [],
    
    outputEndpoints: [],
    
    render: function() {
        var json = this.model.toJSON();
        json.label = this.label || json.name;
        json.tooltip = this._createToolTip(json.name);
        var html = this.template(json);
        this.$('.operator-label').tooltip('destroy');
        this.$el.html(html);
        this.$('.operator-label').tooltip({
            track: true
        });
        return this;
    },
    
    updateEndpoints: function() {
        var json = this.model.toJSON();
        json.label = this.label || json.name;
        this._removeAllEndpoints();
        this._setInputPorts( json.inputPorts || [] );
        this._setOutputPorts( json.outputPorts || [] );
    },
    
    _createToolTip: function(name) {
        var string = 'Class Name: \n';
        string += name ? name.replace(/\.([^\.]+)$/, '.\n$1') : 'no name';
        return string;
    },
    
    _removeAllEndpoints: function() {
        var iEPs = this.model.get('inputEndpoints') || [];
        var oEPs = this.model.get('outputEndpoints') || [];
        _.each(iEPs.concat(oEPs), function(ep) {
            this.plumb.deleteEndpoint(ep);
        }, this);
    },
    
    _setInputPorts: function(ports) {
        this.model.set('inputEndpoints', this._createEndpoints(ports, inputEndpointOptions, 0, -1) );
    },
    
    _setOutputPorts: function(ports) {
        this.model.set('outputEndpoints', this._createEndpoints(ports, outputEndpointOptions, 1, 1) );
    },
    
    _createEndpoints: function(ports, options, y_pos, y_incident) {
        var endpoints = [];
        for (var i = 0, len = ports.length; i < len; i++) {
            var port = ports[i];
            var endpointOptions = {
                // anchor placement
                anchor: [this._getXPosition(len, i), y_pos, 0, y_incident],
            };
            var endpoint = this.plumb.addEndpoint(this.el, endpointOptions, options);
            var label = endpoint.getOverlay('label');
            label.setLabel(port.name);
            
            
            // DO NOT MODIFY THIS ATTRIBUTE IN ANY WAY
            // connection detection relies on this being the exact portname
            endpoint.canvas.title = port.name;
            
            
            endpoints.push(endpoint);
        }
        return endpoints;
    },
    
    _getXPosition: function(len, idx) {
        if (len === 1) return 0.5;
        // if (len === 2) return idx === 0 ? 0.33 : 0.67 ;
        return (1 / (len - 1)) * idx;
    },
    
    remove: function() {
        delete this.plumb;
        bassview.prototype.remove.call(this);
    },
    
    template: kt.make(__dirname+'/DagOperatorView.html','_')
    
});
exports = module.exports = DagOperatorView;