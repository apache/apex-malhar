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

/*global angular, google*/
(function () {
'use strict';

angular.module('widgets')
  .directive('lineChart', function () {
    return {
      template: '<div></div>',
      scope: {
        chart: '='
      },
      restrict: 'E',
      replace: true,
      link: function postLink(scope, element, attrs) {
        var lineChart = new google.visualization.LineChart(element[0]);

        function draw(chart) {
          var data = chart.data;

          if (!data) {
            data = [];
          }

          var table = new google.visualization.DataTable();
          table.addColumn('datetime');
          table.addColumn('number');
          table.addRows(data.length);

          var view = new google.visualization.DataView(table);

          for (var i = 0; i < data.length; i++) {
            var item = data[i];
            table.setCell(i, 0, new Date(item.timestamp));
            var value = parseFloat(item.value);
            table.setCell(i, 1, value);
          }

          var options;
          if (data.length === 0 && chart.emptyChartOptions) {
            options = chart.emptyChartOptions();
          } else {
            options = chart.options;
          }

          lineChart.draw(view, options);
        }

        scope.$watch('chart', function (chart) {
          if (chart) {
            draw(chart);
          }
        });
      }
    };
  });

})();
