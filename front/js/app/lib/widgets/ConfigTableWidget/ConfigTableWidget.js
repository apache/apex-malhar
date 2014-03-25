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
var BaseView = DT.lib.WidgetView;
var GatewayInfoModel = require('../../../../datatorrent/GatewayInfoModel');
var RestartModal = DT.lib.RestartModal;
var Notifier = DT.lib.Notifier;

/**
 * ConfigTableWidget
 *
*/
var ConfigTableWidget = BaseView.extend({
    
    initialize: function(options) {
        
        BaseView.prototype.initialize.call(this, options);
        this.dataSource = options.dataSource;
        this.issues = options.issues;

        this.listenTo(this.collection, 'sync', this.render);
        this.listenTo(this.issues, 'sync', this.render);
        this.filters = {
            name: '',
            value: '',
            property: ''
        };
    },
    
    html: function() {
        var filters = this.filters;
        var json = {
            filters: filters
        };
        return this.template(json);
    },

    render: function() {
        var html = this.html();
        this.$el.html(html);
        this.updateResults();
        return this;
    },

    updateResults: function() {
        var filters = this.filters;
        var collection = this.collection.filter(function(prop) {
            var passed = true;
            _.each(filters, function(val, key) {
                if (val && prop.get(key).toString().toLowerCase().indexOf(val.toLowerCase()) === -1) {
                    passed = false;
                }
            });
            return passed;
        });
        collection = _.map(collection, function(prop) { return prop.toJSON(); });
        var html = this.results_template({ properties: collection });
        this.$('tbody').html(html);
    },
    
    events: {
        'dblclick .property-value': 'startPropertyEdit',
        'click .update-property': 'updateProperty',
        'click .update-cancel': 'updateCancel',
        'click .restart': 'restart',
        'change .config-properties-filter': 'updateFilters',
        'keyup .config-properties-filter': 'updateFilters'
    },

    startPropertyEdit: function(e) {
        var $span = $(e.target);
        var text = $.trim($span.text());
        var name = $span.data('property-name');
        var $textarea = $('<textarea>' + text + '</textarea>');
        $span.replaceWith($textarea);
        _.defer(function() {
            $textarea.focus();
        });

        var $btnCancel = $('<button role="button" class="btn update-cancel" style="margin-left: 10px;" data-property-name="' + name + '">cancel</button>');
        $btnCancel.insertAfter($textarea);

        var $btn = $('<br><button role="button" class="btn update-property" data-property-name="' + name + '">update</button>');
        $btn.insertAfter($textarea);
    },

    updateProperty: function(e) {
        var $btn = $(e.target);
        var propName = $btn.data('property-name');
        var $td = $btn.parent('td');
        var newVal = $td.find('textarea').val();
        var property = this.collection.get(propName);
        if (property) {
            property.set('value', newVal);
            property.save();
        }
        $td.html('<span class="property-value" data-property-name="' + propName + '">' + newVal + '</span>');
    },

    updateCancel: function(e) {
        var $btn = $(e.target);
        var propName = $btn.data('property-name');
        var $td = $btn.parent('td');
        var property = this.collection.get(propName);
        if (property) {
            $td.html('<span class="property-value" data-property-name="' + propName + '">' + property.get('value') + '</span>');
        }
    },

    restart: function () {
        if (!this.restartModal) {
            this.restartModal = new RestartModal({
                dataSource: this.dataSource,
                message: 'Restarting the Gateway...',
                prompt: true
            });
            this.restartModal.addToDOM();
        }
        this.restartModal.launch();
    },

    updateFilters: _.debounce(function() {
        var filters = this.filters;
        $('.config-properties-filter').each(function(i,el){
            var $e = $(el);
            filters[$e.data('field')] = $e.val();
        });
        this.updateResults();
    }, 300),

    template: kt.make(__dirname+'/ConfigTableWidget.html','_'),

    results_template: kt.make(__dirname+'/ConfigTableResults.html')
    
});

exports = module.exports = ConfigTableWidget;