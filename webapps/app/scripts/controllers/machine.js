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

function getEmptyChartOptionsFn(scope) {
  return function () {
    var now = Date.now();
    var max = new Date(now);
    var min = new Date(now - scope.lookback * 60 * 1000);
    var options = jQuery.extend({}, chartOptions, {
      vAxis: { minValue: 0, maxValue: 100 },
      hAxis: { viewWindow: { min: min, max: max }}
    });
    return options;
  };
}

function chartData(data, property) {
  return _.map(data, function (obj) {
    return {
      timestamp: obj.timestamp,
      value: obj[property]
    };
  });
}

function getRequestParams($scope) {
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

function PollRequest (rest, scope, callback) {
  this.rest = rest;
  this.scope = scope;
  this.max = scope.lookback;
  this.dataCache = null;
  this.callback = callback;
  this.cancelled = false;
  this.interval = null;
  this.timeout = null;
  this.params = getRequestParams(this.scope);
}

PollRequest.prototype = {
  cancel: function () {
    this.cancelled = true;
    this.scope.requestProgress = 0;
    clearInterval(this.interval);
    clearTimeout(this.timeout);
  },

  responseStatus: function () {
    this.scope.requestProgress = Math.round((Date.now() - this.requestStartTime) / 1000);
    this.scope.$apply();
  },

  fetchMachineData: function () {
    if (this.dataCache && this.dataCache.length) {
      this.params.lastTimestamp = _.last(this.dataCache).timestamp;
    }

    var max = this.max;

    this.requestStartTime = Date.now();

    this.interval = setInterval(this.responseStatus.bind(this), 250);

    var that = this;

    this.rest.getMachineData(this.params).then(function (response) {
      if (that.cancelled) {
        return;
      }

      that.scope.response = response;
      var minutes = response.minutes;
      that.scope.requestProgress = 0;
      clearInterval(that.interval);

      var now = Date.now();
      that.scope.lastResponse = new Date(now);
      that.scope.responseTime = now - that.requestStartTime;
      if (!that.dataCache) {
        that.dataCache = minutes;

        that.scope.minutesCached = 0;
        that.scope.minutesReceived = minutes.length;

      } else if (minutes.length > 0) {
        that.scope.minutesCached = that.dataCache.length;
        that.scope.minutesReceived = minutes.length;

        that.dataCache.pop(); // remove last element since response should have new values for the last minute

        var newlength = that.dataCache.length + minutes.length;

        if (newlength > max) {
          that.dataCache.splice(0, newlength - max);
        }
        that.dataCache.push.apply(that.dataCache, minutes); // add all elements
      }

      that.callback(that.dataCache);

      var nextTimeout = settings.machine.pollInterval - (Date.now() - that.requestStartTime);
      nextTimeout = Math.max(0, nextTimeout);

      that.timeout = setTimeout(that.fetchMachineData.bind(that), nextTimeout);
    },
    function (errorResponse) {
      that.cancel();
    });
  }
};


angular.module('machine')
  .controller('MachineController', ['$scope', '$timeout', '$location', '$routeParams', 'rest', function ($scope, $timeout, $location, $routeParams, rest) {
    var queryParams = new URI(window.location.href).query(true);
    var emptyChartOptionsFn = getEmptyChartOptionsFn($scope);

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

    $scope.reload = function () {
      //TODO have Angular route instead of reloading the page
      window.location.href = window.location.pathname + '?' + jQuery.param(getRequestParams($scope));
    };

    function updateCharts(data) {
      if (data && (data.length > 0)) {
        var current = _.last(data);
        $scope.cpu = parseFloat(current.cpu);
        $scope.ram = parseFloat(current.ram);
        $scope.hdd = parseFloat(current.hdd);
      }
      $scope.cpuChart = {
        data: chartData(data, 'cpu'),
        options: chartOptions,
        emptyChartOptions: emptyChartOptionsFn
      };
      $scope.ramChart = {
        data: chartData(data, 'ram'),
        options: chartOptions,
        emptyChartOptions: emptyChartOptionsFn
      };
      $scope.hddChart = {
        data: chartData(data, 'hdd'),
        options: chartOptions,
        emptyChartOptions: emptyChartOptionsFn
      };
    }

    $scope.cpuChart = {
      emptyChartOptions: emptyChartOptionsFn
    };
    $scope.ramChart = {
      emptyChartOptions: emptyChartOptionsFn
    };
    $scope.hddChart = {
      emptyChartOptions: emptyChartOptionsFn
    };

    var request = null;
    function reloadCharts() {
      //TODO check if lookback is valid
      if (request) {
        request.cancel();
      }
      request = new PollRequest(rest, $scope, updateCharts);
      request.fetchMachineData();
    }

    $scope.$watch('[customer, product, os, software1, software1, deviceId]', function () {
      reloadCharts();
    }, true);

    var lookbackTimeout;

    var firstUpdate = true;

    $scope.$watch('lookback', function () {
      if (!firstUpdate) { // skip first change since there is a watch for select fields
        clearTimeout(lookbackTimeout);
        lookbackTimeout = setTimeout(reloadCharts, 500);
      } else {
        firstUpdate = false;
      }
    });

    // stop server polling on destroy (e.g. when navigating to another view)
    $scope.$on('$destroy', function () {
      if (request) {
        request.cancel();
      }
    }.bind(this));
  }]);

})();
