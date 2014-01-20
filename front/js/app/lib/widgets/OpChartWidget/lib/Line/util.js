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
var parent_util = require('../util');
var _ = require('underscore');
var BigRat = require('big-rational');
var BigInteger = require('jsbn');

exports.scaleFraction = parent_util.scaleFraction;
exports.transformTupleData = function(data) {
    // Comes in like this:
    // [{
    //     "[SERIES1]": {
    //          "[X_VALUE]": "[Y_VALUE]"
    //     },
    //     "[SERIES2]": {
    //          "[X_VALUE]": "[Y_VALUE]"
    //     },
    //     "[SERIES3]": {
    //          "[X_VALUE]": "[Y_VALUE]"
    //     }
    // },…]
    
    // Should leave like this:
    // [
    //     {
    //         "x_key": "[X_VALUE]",
    //         "[SERIES1]": "[Y_VALUE]",
    //         "[SERIES2]": "[Y_VALUE]",
    //         "[SERIES3]": "[Y_VALUE]"
    //     }
    // ]
    
    var x_values = {};
    
    data = [].concat(data);
    
    for (var i=0; i < data.length; i++) {
        
        var tdata = data[i];
        
        for ( var series_key in tdata ) {

            var series = tdata[series_key];

            for ( var x_value in series ) {

                var y_value = series[x_value];

                if ( ! x_values[x_value] ) {
                    x_values[x_value] = { 'x_key' : x_value*1 }
                }

                x_values[x_value][series_key] = y_value*1;

            }

        }
    }
    
    var result = _.values(x_values);
    return result;
}
exports.extractPlots = function(recProperties, tupledata) {
    
    // {
    //     chartType: "LINE",
    //     class: "com.malhartech.demos.chart.YahooFinanceApplication$YahooFinanceTimeSeriesAverageChartOperator",
    //     name: "AverageChart",
    //     xAxisLabel: "TIME",
    //     yAxisLabel: "PRICE",
    //     yNumberType: "FLOAT"
    // }
    
    // {
    //     "[SERIES1]": {
    //          "[X_VALUE]": "[Y_VALUE]"
    //     },
    //     "[SERIES2]": {
    //          "[X_VALUE]": "[Y_VALUE]"
    //     },
    //     "[SERIES3]": {
    //          "[X_VALUE]": "[Y_VALUE]"
    //     }
    // }
    
    // key: "",
    // // This is the attribute in the data 
    // // points that constitutes the y-value.
    // 
    // color: "",
    // // The color of the plotted elements in
    // // this series.
    // 
    // lower: undefined,
    // // The lower extremum (minimum) of this plot's values
    // 
    // upper: undefined
    // // Thie higher extremum (maximum) of this plot's values
    
    var colors = [
        '#3cda52',
        '#1a9dda',
        '#da1c17',
        '#d848da',
        '#da3b68',
        '#f2be20',
        '#e58406',
        '#17d0d7',
        '#5769d7'
    ];
    
    var plots = [];
    
    for ( var series_key in tupledata ) {
        plots.push({
            'key': series_key,
            'color': colors.shift()
        });
    }
    console.log('plots', plots);
    return plots;
    
}