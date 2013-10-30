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

/*global settings, angular, google, jQuery, _, URI*/
(function () {
'use strict';

var chartOptions = {
  legend: 'none',
  vAxis: { format: settings.machine.metricformat },
  chartArea: { top: 20, height: 240 }
};

function chartData(data, property) {
  return _.map(data, function (obj) {
    return {
      timestamp: obj.timestamp,
      value: obj[property]
    };
  });
}

angular.module('machine')
  .controller('MachineController', ['$scope', '$timeout', '$location', '$routeParams', 'rest', function ($scope, $timeout, $location, $routeParams, rest) {
    var queryParams = new URI(window.location.href).query(true);

    $scope.app = rest.getApp(settings.machine.appName);
    $scope.$watch('app', function (app) {
      if (app) {
        $scope.appURL = settings.appsURL + app.id;
      }
    });

    $scope.cpu = 0;
    $scope.ram = 0;
    $scope.hdd = 0;

    $scope.range = function (name) {
      var r = settings.machine.range[name];
      return _.range(r.start, r.stop + 1);
    };

    function setupSelect(name, label) {
      var rangeValues = $scope.range(name);
      var list = _.map(rangeValues, function (value) {
        return {
          value: String(value),
          label: label + ' ' + value
        };
      });
      list.splice(0, 0, { value: "", label: 'ALL '});

      $scope.select[name] = list;

      var selected = null;

      if (queryParams[name]) {
        selected = _.findWhere(list, { value: queryParams[name] });
      }

      if (selected) {
        $scope[name] = selected;
      } else {
        $scope[name] = list[0];
      }
    }

    $scope.select = {};
    setupSelect('customer', 'Customer');
    setupSelect('product', 'Product');
    setupSelect('os', 'OS');
    setupSelect('software1', 'Software1 Version');
    setupSelect('software2', 'Software2 Version');
    setupSelect('deviceId', 'Device ID');
    $scope.lookback = queryParams.lookback ? parseInt(queryParams.lookback, 10) : settings.machine.lookback;

    function getParams() {
      return {
        customer: $scope.customer.value,
        product: $scope.product.value,
        os: $scope.os.value,
        software1: $scope.software1.value,
        software2: $scope.software2.value,
        deviceId: $scope.deviceId.value,
        lookback: $scope.lookback
      };
    }

    $scope.reload = function () {
      //$location.path('/home/2/customer/5');
      //console.log(window.location);
      //TODO
      window.location.href = window.location.pathname + '?' + jQuery.param(getParams());

      //window.location.href = window.location.pathname + '#/?customer=' + $scope.customer.value
      //    + '&product=' + $scope.product.value;
    };

    $scope.$watch('machineData', function (data) {
      if (data && (data.length > 0)) {
        var current = _.last(data);
        $scope.cpu = parseFloat(current.cpu);
        $scope.ram = parseFloat(current.ram);
        $scope.hdd = parseFloat(current.hdd);

        $scope.cpuChart = {
          data: chartData(data, 'cpu'),
          options: chartOptions
        };
        $scope.ramChart = {
          data: chartData(data, 'ram'),
          options: chartOptions
        };
        $scope.hddChart = {
          data: chartData(data, 'hdd'),
          options: chartOptions
        };
      }
    });

    var dataCache;

    function fetchMachineData() {
      var params = getParams();

      if (dataCache && dataCache.length) {
        params.lastTimestamp = _.last(dataCache).timestamp;
      }

      var max = $scope.lookback;

      rest.getMachineData(params).then(function (response) {
        if (!dataCache) {
          dataCache = response;
        } else {
          var newlength = dataCache.length + response.length;
          if (newlength > max) {
            dataCache.splice(0, newlength - max);
            dataCache.push.apply(dataCache, response); // add all elements
          }
        }
        $scope.machineData = dataCache;
        $timeout(fetchMachineData, 1000);
      });
    }

    fetchMachineData();
  }]);

})();
