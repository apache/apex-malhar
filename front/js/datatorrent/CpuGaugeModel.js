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
var Backbone = require('backbone');

var CpuGaugeModel = Backbone.Model.extend({
    defaults: {
        value: 0
    },

    initialize: function(attributes, options) {
        this.operators = options.operators;
        this.listenTo(this.operators, 'change', this.update);
    },

    update: function () {
        var operators = this.operators;
        var value;

        if (operators.length) {
            var sum = operators.reduce(function (memo, operator) {
                return memo + parseFloat(operator.get('cpuPercentageMA'));
            }, 0);

            value = Math.round(sum/operators.length);
        } else {
            value = 0;
        }

        this.set('value', value);
    }
});

exports = module.exports = CpuGaugeModel;