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

/*global settings, angular, jQuery, _*/
(function () {
'use strict';

angular.module('twitter')
    .controller('TwitterController', ['$scope', 'rest', function ($scope, rest) {
        $scope.app = rest.getApp(settings.twitter.appName);

        $scope.$watch('app', function (app) {
            if (app) {
                $scope.appURL = settings.appsURL + app.id;
            }
        });
    }])
    .controller('TwitterGridControlller', ['$scope', 'socket', function ($scope, socket) {
        socket.subscribe(settings.twitter.topic, function(data) {
            var list = [];
            jQuery.each(data.data, function(key, value) {
                list.push( { name: key, value: parseInt(value, 10) } );
            });
            list = _.sortBy(list, function(item) {
                return -item.value;
            });
            $scope.myData = list;
            $scope.$apply();
        });

        $scope.gridOptions = {
            data: 'myData',
            enableColumnResize: true,
            columnDefs: [{ field: "name", displayName: 'URL', width: '75%', sortable: false },
                { field: "value", displayName: 'Count', width: '25%', sortable: false }]
        };
    }])
    .controller('TwitterBarChartController', ['$scope', 'socket', function($scope, socket) {
        socket.on(settings.twitter.topic, function(data) {
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
                item.name += ' - ' + item.value;
                item.score = item.value / max * 100;
            });

            $scope.twitterBarChartData = list;
            $scope.$apply();
        });
        $scope.twitterBarChartData = [];
    }]);

})();
