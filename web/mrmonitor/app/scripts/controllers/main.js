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
    $scope.showLoading = false;

    function jobRequest(jobId, command) {
      var id = util.extractJobId(jobId);

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
        if ($scope.jobs.hasOwnProperty(activeJobId)) {
          $scope.job = $scope.jobs[activeJobId];
          $scope.showLoading = false;
        } else {
          $scope.showLoading = true;
        }
        jobQueryRequest(activeJobId); // resend request since finished jobs receive only 1 update
      } else  {
        $scope.showLoading = false;

        /*
        // dev only, automatic map reduce job discovery
        // will be invoked on URL #/jobs/

        rest.getApp().then(function (app) {
          if (app && app.id) {
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
        */
      }
    });

    function jobRemoved (id) {
      var jobId = (id.indexOf('job_') < 0) ? 'job_' + id : id;

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

      if (data.mapHistory || data.reduceHistory) {
        if ($scope.activeJobId && (data.id === $scope.activeJobId)) {
          $scope.$broadcast('history', data);
        }
      }

      var job = data.job;

      if (!job) {
        return;
      }

      $scope.jobs[job.id] = job;
      $scope.job = job;

      if ($scope.activeJobId === job.id) {
        $scope.showLoading = false;
      }

      $scope.$apply();
    }, $scope);
  })
  .controller('MonitoredJobGridController', function ($scope, util, $templateCache, $state, notificationService) {
    function updateGrid () {
      var list = _.map(_.values($scope.jobs), function (job) {
        return {
          id: job.id,
          name: job.name,
          cellRowClass: ($scope.activeJobId && ($scope.activeJobId === job.id)) ? 'row-active': '',
          state: job.state,
          mapProgress: job.mapProgress,
          reduceProgress: job.reduceProgress,
          startTime: job.startTime
        };
      });

      list = _.sortBy(list, function (job) {
        return (- job.startTime);
      });

      /*
       var makeActiveFirst = true;

       if (makeActiveFirst && $scope.activeJobId) {
        var activeJobId = $scope.activeJobId;
        console.log('activeJobId ' + activeJobId);
        console.log(list);
        var activeJob = _.find(list, function (job) {
          console.log(job);
          return job.id === activeJobId;
        });
        console.log('activeJob ' + activeJob);
        console.log(list);
        if (activeJob) {
          list = _.without(list, activeJob);

          list.splice(0, 0, activeJob);
          //makeActiveFirst = false;
        }
      }
      */

      $scope.gridData = list;
    }

    $scope.$watch('job', function (job) {
      if (!job) {
        return;
      }

      updateGrid();
    });

    $scope.$on('jobRemoved', function (event, id) {
      delete $scope.jobs[id];
      updateGrid();
    });

    $scope.$watch('activeJobId', function () {
      updateGrid();
    });

    var linkTemplate = '<div class="ngCellText" ng-class="col.colIndex()"><span ng-cell-text><a href="#/jobs/{{COL_FIELD}}">{{COL_FIELD}}</a></span></div>';

    var rowTemplate = $templateCache.get('rowTemplate.html');
    rowTemplate = rowTemplate.replace('ngCell', 'ngCell {{row.entity.cellRowClass}}'); // custom row class

    $scope.removeJob = function (id) {
      $scope.jobRemoveRequest(id);

      notificationService.notify({
        title: 'Remove Job',
        text: 'Request to remove job ' + id + ' from monitoring has been sent.',
        type: 'info',
        delay: 3000,
        icon: false,
        history: false
      });
    };

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

