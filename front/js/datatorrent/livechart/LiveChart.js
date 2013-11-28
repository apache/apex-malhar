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
var Backbone = require('backbone');
var LiveChartModel = require('./lib/LiveChartModel');
var LiveChartView = require('./lib/LiveChartView');

// todo
// ----
// [X] guides
// [X] resizable
// [X] pause and continue
// [-] turn series visibility on and off
// [-] improve resizer look
// [-] programmatically get and set series
// [-] programmatically set colors
// [-] mouse overlay

// constructor
// new LiveChart([/* data */], { /* options */ });
// new LiveChart({ /* options */ });

function LiveChart() {
    
    var data, options;
    
    // Data may have been provided as an argument
    // instead of an option or not at all.
    if (arguments.length === 2) {
        options = arguments[1];
        options.data = arguments[0] || [];
    } else {
        options = arguments[0];
        options.data = options.data || [];
    }
    
    // Create the model and the view.
    this.model = new LiveChartModel(options || {});
    this.view = new LiveChartView({
        model: this.model,
        el: this.model.get('el')
    });
    
    // Set a reference to the data object on this object.
    this.data = this.model.get('data');
}

// Set the svg element
LiveChart.prototype = {
    
    // PUBLIC METHODS

    // Draws the data
    render: function(data) {
        return this.view.render(data);
    },
    
    // Pauses the updates
    pause: function() {
        this.model.set('paused', true);
    },
    
    // Unpauses the updates
    unpause: function() {
        this.model.set('paused', false);
    },
    
    // Sets a plot or a collection of plots
    plot: function(plots) {
        this.model.plot(plots);
    },
    
    // Unplots based on the plot key
    unplot: function(key) {
        this.model.unplot(key);
    }
    
}

exports = module.exports = LiveChart;