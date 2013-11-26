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
 * Alert Dag View
 * 
 * Visualizes the new dag additions
 * when adding an alert.
*/

var Base = require('../../DagView');
var AlertDagView = Base.extend({
    
    initialize: function(options) {
        Base.prototype.initialize.call(this, options);
        
        // listen for changes on the filter and 
        // escalation classes for auto-connection
        this.listenTo(this.model.get('filter'), 'change:outputEndpoints', this.connectFilterEscalation);
        this.listenTo(this.model.get('escalation'), 'change:inputEndpoints', this.connectFilterEscalation);
        
        // Listen to connection changes
        this.on('connection', this._onAlertConnection);
        this.on('disconnection', this._onAlertDisconnection);
    },
    
    _onAlertConnection: function(connection) {
        // only do something if the source is the escalation and the target is NOT the filter
        if (
            connection.source.operator !== this.model.get('escalation') || 
            connection.target.operator === this.model.get('filter')
        ) {
            return;
        }
        
        var action = connection.target.operator;
        
        action.set({
            'outputPort': connection.source.port,
            'inputPort': connection.target.port
        });
    },
    
    _onAlertDisconnection: function(connection) {
        // do nothing if not a registered connection
        if (!connection) return;
        
        // only do something if the source is the escalation and the target is NOT the filter
        if (
            connection.source.operator !== this.model.get('escalation') || 
            connection.target.operator === this.model.get('filter')
        ) {
            return;
        }
        
        var action = connection.target.operator;
        
        action.unset('outputPort');
        action.unset('inputPort');
    },
    
    connectFilterEscalation: function() {
        var filterEP = this.model.get('filter').get('outputEndpoints') || [];
        var escalationEP = this.model.get('escalation').get('inputEndpoints') || [];
        
        if (filterEP.length !== 1) return;
        filterEP = filterEP[0];
        filterEP.setEnabled(false); // ensure that they can't be used for anything else
        
        
        if (escalationEP.length !== 1) return;
        escalationEP = escalationEP[0];
        filterEP.setEnabled(true); // ensure that they can't be used for anything else
        
        this.plumb.connect({
            source: filterEP,
            target: escalationEP,
            detachable: false
        });
    },
    
    render: function() {
        
        this.clearAllOperators();
        
        // add filter class
        var filter = this.model.get('filter');
        this.addOperator(filter, { label: "condition", x: "middle", y: 40 });
        
        // add escalation class
        var escalation = this.model.get('escalation');
        this.addOperator(escalation, { label: "escalation", x: "middle", y: 220 });
        
        // add action classes
        var actions = this.model.get('actions');
        var positions = ['left', 'middle', 'right'];
        var y_interval = 180;
        for (var i = 0; i < actions.length; i++) {
            var action = actions[i];
            var pos = positions[i % 3];
            var label = 'action' + i;
            var y = (Math.floor(i / 3) * y_interval) + 400;
            this.addOperator(action, { label: label, x: pos, y: y });
        };
        
        // set the affix behavior and dimensions explicitly
        this.$el.css({
            'height': this.containerHeight,
            'width': this.containerWidth
        }).affix({
            'offset': {
                'top': 200,
                'y': 10
            }
        });
        
        return this;
    },
    
    operatorDimensions: [85, 85],
    
    containerWidth: 446,
    
    containerHeight: 530,
    
    containerPadding: {
        top: 5,
        right: 20,
        bottom: 5,
        left: 20
    },
    
});

exports = module.exports = AlertDagView;