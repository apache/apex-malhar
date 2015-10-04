/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*global settings, angular, jQuery, _*/
(function () {
'use strict';

angular.module('twitter')
    .controller('TwitterUrlsController', ['$scope', 'rest', function ($scope, rest) {
        rest.getApp(settings.twitterUrls.appName).then(function (app) {
            $scope.app = app;
            $scope.appURL = settings.appsURL + app.id;
        });

        $scope.topic = settings.twitterUrls.topic;
        $scope.pageTitle = 'Twitter Top URLs';
        $scope.entity = 'URLs';
        $scope.gridTitle = 'Twitter Top URLs';
        $scope.chartTitle = 'Top 10 URLs Chart';
        $scope.colName = 'URL';
        $scope.formatter = function(url) {
            return '<a class="svg-link" xlink:href="' + url + '">' + url + '</a>';
        };
    }])
    .controller('TwitterHashtagsController', ['$scope', 'rest', function ($scope, rest) {
        rest.getApp(settings.twitterHashtags.appName).then(function (app) {
          $scope.app = app;
          $scope.appURL = settings.appsURL + app.id;
        });

        $scope.topic = settings.twitterHashtags.topic;
        $scope.pageTitle = 'Twitter Top Hashtags';
        $scope.entity = 'hashtags';
        $scope.gridTitle = 'Twitter Top Hashtags';
        $scope.chartTitle = 'Top 10 Hashtags Chart';
        $scope.colName = 'Hashtag';
        $scope.formatter = function(Hashtag) {
            return '<a class="svg-link" xlink:href="https://twitter.com/search?q=%23' + encodeURIComponent(Hashtag) + '">' + Hashtag + '</a>';
        };
    }])
    .controller('TwitterGridControlller', ['$scope', 'socket', function ($scope, socket) {
        socket.subscribe($scope.topic, function(data) {
            var list = [];
            jQuery.each(data.data, function(key, value) {
                list.push( { name: key, value: parseInt(value, 10) } );
            });
            list = _.sortBy(list, function(item) {
                return -item.value;
            });
            $scope.topTen = list;
            $scope.$apply();
        }, $scope);

        $scope.gridOptions = {
            data: 'topTen',
            enableColumnResize: true,
          columnDefs: [
            { field: "name", displayName: $scope.colName, width: '75%', sortable: false },
            { field: "value", displayName: 'Count', width: '25%', sortable: false }
          ]
        };
    }])
    .controller('TwitterBarChartController', ['$scope', 'socket', function($scope, socket) {
        socket.subscribe($scope.topic, function(data) {
            var list = [];
            jQuery.each(data.data, function(key, value) {
                list.push( { name: key, value: parseInt(value, 10) } );
            });
            list = _.sortBy(list, function(item) {
                return -item.value;
            });
            //var max = _.max(list, function(item) {item.value});
            var max = list[0].value;
            _.each(list, function(item) {
                if ($scope.formatter) {
                    item.name = $scope.formatter(item.name);
                }
                item.name += ' - ' + item.value;
                item.score = item.value / max * 100;
            });

            $scope.twitterBarChartData = list;
            $scope.$apply();
        }, $scope);
        $scope.twitterBarChartData = [];
    }]);

})();
