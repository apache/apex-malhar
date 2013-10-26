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

function translateLatLong(item) {
    var match = item.location.match(/\((\d+),(\d+)/); //TODO server should pass this data as numbers
    var lat = parseInt(match[1], 10);
    var lon = parseInt(match[2], 10);
    var phone = parseInt(item.phone, 10);

    //TODO magic numbers
    var latitude = 37.40180101292334 + (phone % 4 - 2) * 0.01 - lat * 0.005;
    var longitude = -121.9966721534729 + (phone % 8 - 4) * 0.01 + lon * 0.007;

    return { latitude: latitude, longitude: longitude, label: item.phone, phone: item.phone };
}

angular.module('mobile')
    .controller('MobileController', ['$scope', 'rest', 'socket', function ($scope, rest, socket) {
        $scope.app = rest.getApp(settings.mobile.appName);

        $scope.$watch('app', function (app) {
            if (app) {
                $scope.appURL = settings.appsURL + app.id;
            }
        });

        var map = {};

        socket.subscribe(settings.mobile.topic.out, function(message) {
            var item = message.data;

            if (!item.hasOwnProperty('removed')) {
                var latlon = translateLatLong(item);
                map[item.phone] = latlon;
            } else {
                delete map[item.phone];
            }

            $scope.$broadcast('datachanged', map);
        });

        //$scope.$on('phone_added', function (event, phone) {
        //    map[phone] = { phone: phone };
        //    $scope.$broadcast('datachanged', map);
        //});

        //$scope.$on('phone_removed', function (event, phone) {
        //    removed[phone] = Date.now();
        //    delete map[phone];
        //    $scope.$broadcast('datachanged', map);
        //});
    }])
    .controller('MobileGridControlller', ['$scope', '$filter', '$timeout', 'socket', function ($scope, $filter, $timeout, socket) {
        $scope.$on('datachanged', function (event, map) {
            $scope.gridData = _.values(map);
        });

        $scope.phone = '';
        $scope.addPhone = function () {
            var command = {
                command : 'add',
                phone : $scope.phone
            };

            var message = { "type" : "publish", "topic" : settings.mobile.topic.in, "data" : command };
            socket.send(message);

            //$scope.$emit('phone_added', $scope.phone);

            $scope.phone = '';
        };

        $scope.removePhone = function(phone) {
            var command = {
                command : 'del',
                phone : phone
            };

            var message = { "type" : "publish", "topic" : settings.mobile.topic.in, "data" : command };
            socket.send(message);

            //$scope.$emit('phone_removed', phone);
        };

        $scope.gridOptions = {
            data: 'gridData',
            enableColumnResize: true,
            enableRowSelection: false,
            columnDefs: [
                { field: "phone", displayName: 'Phone', width: '30%', sortable: false },
                { field: "latitude", displayName: 'Latitude', cellFilter: 'number:3', width: '30%', sortable: false },
                { field: "longitude", displayName: 'Longitude', cellFilter: 'number:3', width: '30%', sortable: false },
                { field: "phone", displayName: '', cellTemplate: '<div class="ngCellText" ng-class="col.colIndex()" ng-click="removePhone(COL_FIELD)"><i class="icon-trash"></i></div>', cellClass: 'mobile-grid-remove', width: '10%', sortable: false }
            ]
        };
    }])
    .controller('MapController', ['$scope', 'socket', function ($scope, socket) {
        google.maps.visualRefresh = true;

        $scope.$on('datachanged', function (event, map) {
            $scope.markersProperty = _.values(map); //TODO update only changed marker
        });

        angular.extend($scope, {
            position: {
                coords: {
                    latitude: 37.36197126180853,
                    longitude: -121.92674696445465
                }
            },
            zoomProperty: 12
        });
    }]);

})();
