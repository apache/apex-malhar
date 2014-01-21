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

angular.module('app', ['ui.router', 'ngAnimate', 'app.service', 'app.directive', 'app.controller', 'app.filter', 'ui.bootstrap']);

angular.module('app')
  .constant('settings', window.settings)
  .config(function ($stateProvider, $urlRouterProvider, webSocketProvider) {
    webSocketProvider.setWebSocketURL(settings.webSocketURL);
    //webSocketProvider.setWebSocketURL('ws://' + window.location.host + '/sockjs/websocket');

    $urlRouterProvider.otherwise('/jobs/');

    $stateProvider
      .state('jobs', {
        url: '/jobs',
        templateUrl: 'views/main.html',
        controller: 'MainCtrl'
      })
      .state('jobs.job', {
        url: '/:jobId',
        templateUrl: 'views/job.html',
        controller: 'JobCtrl'
      })
      .state('appList', {
        url: '/apps',
        templateUrl: 'views/apps.html',
        controller: 'AppListCtrl'
      });
  }).run (function ($templateCache) {
    // workaround for angular-bootstrap Progressbar directive issue with ngAnimate (ng-repeat is removed from the template)
    $templateCache.put('template/progressbar/progress.html',
      '<div class=\"progress\"><progressbar width=\"bars[0].to\" old=\"bars[0].from\" animate=\"bars[0].animate\" type=\"bar.type\"></progressbar></div>');
  });