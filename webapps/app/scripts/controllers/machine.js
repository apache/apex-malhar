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

/*global settings, angular, google, jQuery, _*/
(function () {
'use strict';

//TODO implement AngularJS directive for google charts
function drawChart(data, options) {
    var table = new google.visualization.DataTable();
    table.addColumn('datetime', 'Time');
    table.addColumn('number');
    table.addRows(data.length);

    var chart = new google.visualization.ScatterChart(document.getElementById(options.container));
    var view = new google.visualization.DataView(table);

    var property = options.property;
    for(var i=0; i < data.length; i++)
    {
        var item = data[i];
        table.setCell(i, 0, new Date(item.timestamp));
        var value = parseFloat(item[property]);
        table.setCell(i, 1, value);
    }

    var chartOptions = { lineWidth: 1, pointSize: 0, legend: 'none', height: 300,
        vAxis: { format: settings.machine.metricformat },
        chartArea: { top: 20, height: 240 } };

    chart.draw(view, chartOptions);
}

angular.module('machine')
    .controller('MachineController', ['$scope', '$timeout', 'rest', function ($scope, $timeout, rest) {
        $scope.app = rest.getApp(settings.machine.appName);

        $scope.$watch('app', function (app) {
            if (app) {
                $scope.appURL = settings.appsURL + app.id;
            }
        });

        $scope.customer = "";
        $scope.product = "";
        $scope.os = "";
        $scope.software1 = "";
        $scope.software2 = "";
        $scope.deviceId = "";
        $scope.lookback = 30;

        $scope.cpu = 0;
        $scope.ram = 0;
        $scope.hdd = 0;

        $scope.range = function (name) {
            var r = settings.machine.range[name];
            return _.range(r.start, r.stop + 1);
        };

        $scope.$watch('machineData', function (data) {
            if (data && (data.length > 0)) {
                drawChart(data, {
                    container: 'cpuChart',
                    property: 'cpu'
                });
                drawChart(data, {
                    container: 'ramChart',
                    property: 'ram'
                });
                drawChart(data, {
                    container: 'hddChart',
                    property: 'hdd'
                });

                var current = _.last(data);
                $scope.cpu = parseFloat(current.cpu);
                $scope.ram = parseFloat(current.ram);
                $scope.hdd = parseFloat(current.hdd);
            }
        });

        function fetchMachineData () {
            var query = {
                customer: $scope.customer,
                product: $scope.product,
                os: $scope.os,
                software1: $scope.software1,
                software2: $scope.software2,
                deviceId: $scope.deviceId,
                lookback: $scope.lookback
            };

            $scope.machineData = rest.getMachineData(query);
            $timeout(fetchMachineData, 1000);
        }

        fetchMachineData();
    }]);

})();
