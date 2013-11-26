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
 * AddAlertWidget
 * 
 * This widget is in charge of creating a new alert
*/

var _ = require('underscore');
var settings = DT.settings;
var kt = require('knights-templar');
var BaseView = DT.widgets.Widget;
var AlertModel = DT.lib.AlertModel;
var Bbind = DT.lib.Bbindings;
var OperatorClassModel = DT.lib.OperatorClassModel;
var OperatorClassCollection = DT.lib.OperatorClassCollection;

// subviews
var TargetStreamView = require('./TargetStreamView');
var FilterSubView = require('./FilterSubView');
var EscalationSubView = require('./EscalationSubView');
var ActionSubView = require('./ActionSubView');
var AlertDagView = require('./AlertDagView');

var AddAlertWidget = BaseView.extend({

    initialize:function(options) {
        
        // Call parent class constructor
        BaseView.prototype.initialize.call(this,options);
        
        // Injections: app instance object and dataSource
        this.instance = options.instance;
        this.dataSource = options.dataSource;
        
        // Common variables for re-use
        var appId = this.instance.get('id');
        var classModelAttrs = { appId: appId };
        var classModelOptions = { dataSource: this.dataSource, appId: appId };
        
        // Set up the new alert model
        this.model = new AlertModel({
            name: '',
            operatorName: '',
            streamName: '',
            filter: new OperatorClassModel(classModelAttrs, classModelOptions),
            escalation: new OperatorClassModel(classModelAttrs, classModelOptions),
            actions: [],
            saveAs: ''
        });
        
        // Set up the operator class collections.
        // These hold the choices for the filter, escalation, and action operators.
        this.filterClasses = new OperatorClassCollection([]);
        this.escalationClasses = new OperatorClassCollection([]);
        this.actionClasses = new OperatorClassCollection([]);
        
        // Bind the alert's name and saveAs inputs
        this.subview('alertName', new Bbind.text({
            model: this.model,
            attr: 'name',
            classElement: function($el) {
                return $el.parent();
            },
            errorClass: 'error'
        }));
        this.subview('saveAs', new Bbind.text({
            model: this.model,
            attr: 'saveAs',
            classElement: function($el) {
                return $el.parent();
            },
            errorClass: 'error'
        }));
        
        // Populate the operator class collections
        _.each(['filter','escalation', 'action'], function(classType) {
            this.dataSource.getAlertClasses({
                classType: classType,
                appId: this.instance.get('id'),
                success: _.bind(function(res) {
                    this[classType + 'Classes'].reset(_.map(res, function(className) { return { name: className } } ));
                }, this)
            });
        }, this);
        
        // Set the subviews
        this.subview('targetStream', new TargetStreamView({
            model: this.model,
            instance: this.instance,
            dataSource: this.dataSource
        }));
        this.subview('filter', new FilterSubView({
            collection: this.filterClasses,
            model: this.model
        }));
        this.subview('escalation', new EscalationSubView({
            collection: this.escalationClasses,
            model: this.model
        }));
        this.subview('dagview', new AlertDagView({
            model: this.model
        }));
        
        // Set the action classes and subviews
        var actions = this.model.get('actions');
        for (var i = 0, max = settings.maxAlertActions; i < max; i++) {
            actions.push(new OperatorClassModel(classModelAttrs, classModelOptions));
            this.subview('action' + (i + 1), new ActionSubView({
                collection: this.actionClasses,
                model: this.model,
                index: i
            }));
        }
        this.model.set('actions', actions);
        
    },

    render: function() {
        var json = {
            app: this.instance.toJSON(),
            alert: this.model.toJSON(),
            settings: settings
        };
        var html = this.template(json);
        var assignments = {
            '#alertName':'alertName',
            '#saveAs': 'saveAs',
            '#filter-section': 'filter',
            '#escalation-section': 'escalation',
            '.dag-container': 'dagview',
            '.streamName': 'targetStream'
        }
        for (var i = 0, max = settings.maxAlertActions; i < max; i++) {
            assignments['.alertAction' + ( i + 1 )] = 'action' + ( i + 1 );
        }
        this.$el.html(html);
        this.assign(assignments);
        return this;
    },
    
    events: {
        'change #operatorName': 'changeTargetOperator',
        'change #streamName': 'changeTargetStream',
        'click #alert_submit_btn': 'submitAlert'
    },
    
    submitAlert: function(evt) {
        // prevent form submission
        evt.preventDefault();
        
        console.log('submitAlert');
    },
    
    changeTargetOperator: function(evt) {
        var operatorName = evt.target.value;
        this.model.set('operatorName', operatorName);
    },
    
    changeTargetStream: function(evt) {
        var streamName = evt.target.value;
        this.model.set('streamName', streamName);
    },
    
    template: kt.make(__dirname+'/AddAlertWidget.html','_')
    
});
exports = module.exports = AddAlertWidget;
