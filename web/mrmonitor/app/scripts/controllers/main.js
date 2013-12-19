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
  .controller('MainCtrl', function ($scope, $state, $stateParams, webSocket, rest, util, notificationService, settings) {
    $scope.jobs = {};

    function jobRequest(id, command) {
      var jsonData = {
        'command': command,
        'hostname': settings.hadoop.host,
        'app_id': id,
        'job_id': id,
        'hadoop_version': settings.hadoop.version,
        'api_version': settings.hadoop.api,
        'rm_port': settings.hadoop.resourceManagerPort,
        'hs_port': settings.hadoop.historyServerPort
      };

      var topic = 'contrib.summit.mrDebugger.mrDebuggerQuery';
      var msg = { type: 'publish', topic: topic, data: jsonData };
      webSocket.send(msg);
    }

    function jobQueryRequest (id) {
      jobRequest(id, 'add');
    }

    $scope.jobRemoveRequest = function (id) {
      jobRequest(id, 'delete');
    };

    $scope.$on('activeJobId', function (event, activeJobId) {
      $scope.activeJobId = activeJobId;

      if (activeJobId) {
        jobQueryRequest(activeJobId); // resend request since finished jobs receive only 1 update
      } else  {
        //TODO
        //dev only
        // will be invoked on URL #/jobs/

        if (true) {
          return;
        }
        rest.getApp().then(function (app) {
          if (app && app.id) {
            //$state.go('jobs.job', { jobId: app.id });
            $scope.app = app;

            var id = util.extractJobId(app.id);
            $scope.activeJobId = id;
            jobQueryRequest(id);

            notificationService.notify({
              title: 'Map Reduce Job Found',
              text: 'Monitoring the most recent job ' + id + '. State ' + app.state,
              type: 'success',
              icon: false,
              history: false
            });

            $state.go('jobs.job', { jobId: app.id });
          }
        });
      }
    });

    function jobRemoved (jobId) {
      $scope.$broadcast('jobRemoved', jobId);

      if ($scope.activeJobId && ($scope.activeJobId === jobId)) {
        $scope.activeJobId = null;
        $state.go('jobs.job', { jobId: null });
      }

      notificationService.notify({
        title: 'Job Removed',
        text: 'Job ' + jobId + ' has been removed from monitoring',
        type: 'success',
        delay: 3000,
        icon: false,
        history: false
      });

      $scope.$apply();
    }

    webSocket.subscribe(settings.topic.job, function (message) {
      var data = JSON.parse(message);

      if (data.removed) {
        jobRemoved(data.id);
        return;
      }

      var job = data.job;

      if (!job) {
        return;
      }

      $scope.jobs[job.id] = job;
      $scope.job = job;

      var jobId = util.extractJobId(job.id);

      if ($scope.activeJobId === jobId) {
        $scope.activeJob = $scope.job;
      }

      $scope.$apply();
    }, $scope);
  })
  .controller('MonitoredJobGridController', function ($scope, util, $templateCache, $state, notificationService) {
    function updateGrid () {
      var list = _.map(_.values($scope.jobs), function (job) {
        var jobId = util.extractJobId(job.id);

        return {
          id: job.id,
          name: job.name,
          cellRowClass: ($scope.activeJobId && ($scope.activeJobId === jobId)) ? 'row-active': '',
          state: job.state,
          mapProgress: job.mapProgress,
          reduceProgress: job.reduceProgress,
          startTime: job.startTime
        };
      });

      list = _.sortBy(list, function (job) {
        return (- job.startTime);
      });

      $scope.gridData = list;
    }

    $scope.$watch('job', function (job) {
      if (!job) {
        return;
      }

      updateGrid();
    });

    $scope.$on('jobRemoved', function (event, id) {
      delete $scope.jobs['job_' + id]; //TODO
      updateGrid();
    });

    $scope.$watch('activeJobId', function () {
      updateGrid();
    });

    var linkTemplate = '<div class="ngCellText" ng-class="col.colIndex()"><span ng-cell-text><a href="#/jobs/{{COL_FIELD}}">{{COL_FIELD}}</a></span></div>';

    var rowTemplate = $templateCache.get('rowTemplate.html');
    rowTemplate = rowTemplate.replace('ngCell', 'ngCell {{row.entity.cellRowClass}}'); // custom row class

    //TODO
    $scope.removeJob = function (id) {
      var jobId = util.extractJobId(id);

      $scope.jobRemoveRequest(jobId);

      notificationService.notify({
        title: 'Remove Job',
        text: 'Request to remove job ' + jobId + ' from monitoring has been sent.',
        type: 'info',
        delay: 3000,
        icon: false,
        history: false
      });
    };

    //TODO
    //var actionCellTemplate = $templateCache.get('cellTemplate.html');
    //actionCellTemplate = actionCellTemplate.replace('{{COL_FIELD CUSTOM_FILTERS}}', '<i class="icon-trash"></i>');
    var actionCellTemplate = '<div class="ngCellText" ng-class="col.colIndex()" ng-click="removeJob(row.entity.id);"><i class="icon-trash"></i></div>';

    $scope.gridOptions = {
      data: 'gridData',
      rowTemplate: rowTemplate,
      enableRowSelection: false,
      showFilter: true,
      columnDefs: [
        { field: 'id', displayName: 'Id', width: 200, cellTemplate: linkTemplate },
        { field: 'name', displayName: 'Name'},
        { field: 'state', displayName: 'State' },
        { field: 'mapProgress', displayName: 'Map Progress', cellFilter: 'percentage' },
        { field: 'reduceProgress', displayName: 'Reduce Progress', cellFilter: 'percentage' },
        { field: 'id', displayName: ' ', cellTemplate: actionCellTemplate, cellClass: 'remove', width: '40px' }
      ]
    };
  });

