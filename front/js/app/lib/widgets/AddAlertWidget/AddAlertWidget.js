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
var Notifier = DT.lib.Notifier;
var Backbone = require('backbone');
var settings = DT.settings;
var kt = require('knights-templar');
var BaseView = DT.widgets.Widget;
var AlertModel = DT.lib.AlertModel;
var Bbind = DT.lib.Bbindings;
var AlertTemplateCollection = DT.lib.AlertTemplateCollection;

// subviews
var TargetStreamView = require('./TargetStreamView');
var TemplateSelectView = require('./TemplateSelectView');
var TemplateParamsView = require('./TemplateParamsView');

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
            'appId': appId
        });
        
        // Create an alert template collection and load it
        this.collection = new AlertTemplateCollection([]);
        this.collection.fetch();
        
        // Set the subviews
        this.subview('alertName', new Bbind.text({
            model: this.model,
            attr: 'name',
            classElement: function($el) {
                return $el.parent().parent();
            },
            errorClass: 'error'
        }));
        this.subview('operatorName', new Bbind.select({
            model: this.model,
            attr: 'operatorName', 
            classElement: function($el) {
                return $el.parent().parent();
            },
            errorClass: 'error'
        }));
        this.subview('targetStream', new TargetStreamView({
            model: this.model,
            instance: this.instance,
            dataSource: this.dataSource
        }));
        this.subview('templateSelect', new TemplateSelectView({
            model: this.model,
            collection: this.collection
        }));
        this.subview('templateParams', new TemplateParamsView({
            model: this.model,
            collection: this.collection
        }));
        
        // listen for changes to template selection for loading the template model
        this.listenTo(this.model, 'change:templateName', this._loadTemplateChoice);
        this.listenTo(this.instance, 'change:logicalPlan', this.render);
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
            '#operatorName': 'operatorName',
            '.streamName': 'targetStream',
            '.templateName': 'templateSelect',
            '.templateParams': 'templateParams'
        }
        this.$el.html(html);
        this.assign(assignments);
        return this;
    },
    
    events: {
        'click #alert_submit_btn': 'submitAlert'
    },
    
    submitAlert: function(evt) {
        // prevent form submission
        evt.preventDefault();
        
        var result = this.model.create();
        if (result === false) {
            var vError = this.model.validationError;
            Notifier.error({
                'title': 'Validation errors:',
                'text': _.reduce(vError, function(memo,msg) {
                    return memo === '' 
                        ? memo + msg 
                        : memo + '\n' + msg
                    ;
                },'')
            });
        }
    },

    _loadTemplateChoice: function() {
        
        var templateName, templateModel;
        
        templateName = this.model.get('templateName');
        
        if (!templateName) return;
        
        templateModel = this.collection.get(templateName);
        
        if (!templateModel) return;
        
        this.model.get('parameters').setTemplate(templateModel);
        
        templateModel.fetch();
    },

    template: kt.make(__dirname+'/AddAlertWidget.html','_')
    
});
exports = module.exports = AddAlertWidget;
