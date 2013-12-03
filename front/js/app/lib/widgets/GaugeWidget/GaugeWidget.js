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
 * GaugeWidget
 */

var _ = require('underscore');
var kt = require('knights-templar');
var BaseView = DT.lib.WidgetView;
//var d3gauge = require('d3-gauge');
var Gauge = require('./gauge');

// class definition
var GaugeWidget = BaseView.extend({

    initialize: function (options) {
        // Call super init
        BaseView.prototype.initialize.call(this, options);
        this.label = options.label;

        // Listen for changes
        if (this.model) {
            this.listenTo(this.model, "change", this.update);
        }
    },

    _doAssignmentsD3gauge: function () {
        var gauge = d3gauge(this.$el.find('.widget-content')[0],
            { clazz: 'simple', label:  'Main Mem' });
        gauge.write(39);
    },

    _doAssignments: function () {
        BaseView.prototype._doAssignments.call(this);

        var config = {
            size: 200,
            label: this.label,
            min: 0,
            max: 100,
            minorTicks: 5
        };

        var range = config.max - config.min;
        config.yellowZones = [
            { from: config.min + range * 0.75, to: config.min + range * 0.9 }
        ];
        config.redZones = [
            { from: config.min + range * 0.9, to: config.max }
        ];

        this.gauge = new Gauge(this.$el.find('.widget-content')[0], config);
        this.gauge.render();
        this.gauge.redraw(0);

        return this;
    },

    update: function () {
        var value = this.model.get('value');
        value = value < 0 ? 0 : value;
        value = value > 100 ? 100 : value;
        this.gauge.redraw(value);
    },

    contentClass: 'gauge-widget-content'
});

exports = module.exports = GaugeWidget;