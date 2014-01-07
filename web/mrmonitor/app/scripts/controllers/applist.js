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

'use strict';

angular.module('app.controller')
  .controller('AppListCtrl', function ($scope, rest) {
    $scope.showLoading = true;

    var apps = rest.getApps();

    apps.then(function (apps) {
      $scope.apps = apps;
      $scope.showLoading = false;
    }, function () {
      $scope.showLoading = false;
    });

    var linkTemplate = '<div class="ngCellText" ng-class="col.colIndex()"><span ng-cell-text><a href="#/jobs/{{COL_FIELD | appId}}">{{COL_FIELD}}</a></span></div>';

    $scope.gridOptions = {
      data: 'apps',
      enableRowSelection: false,
      enableColumnResize: true,
      showFilter: true,
      columnDefs: [
        { field: 'id', displayName: 'Task', cellTemplate: linkTemplate, width: 250 },
        { field: 'name', displayName: 'Name' },
        { field: 'user', displayName: 'User' },
        { field: 'queue', displayName: 'Queue' },
        { field: 'startedTime', displayName: 'Start Time', cellFilter: 'date:\'yyyy-MM-dd HH:mm:ss\'', width: 150 },
        { field: 'state', displayName: 'State' },
        { field: 'finalStatus', displayName: 'Final Status' },
        { field: 'progress', displayName: 'Progress', cellFilter: 'percentage' }
      ]
    };
  });