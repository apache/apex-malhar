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

/*global angular*/
(function () {
'use strict';

angular.module('widgets', ['socket']);

angular.module('twitter', ['rest', 'socket', 'widgets', 'ngGrid']);
angular.module('mobile', ['rest', 'widgets', 'ngGrid', 'google-maps']);
angular.module('machine', ['ng', 'rest', 'widgets']);
angular.module('dimensions', ['ng', 'rest', 'widgets']);
angular.module('fraud', ['rest', 'widgets']);

angular.module('app', ['socket', 'twitter', 'mobile', 'machine', 'dimensions', 'fraud']);

angular.module('app')
  .config(function ($routeProvider, socketProvider) {
    if (window.settings) {
      socketProvider.setWebSocketURL(settings.webSocketURL);
    }

    $routeProvider
      .when('/', {
        templateUrl: 'views/welcome.html'
      })
      .when('/twitter', {
        templateUrl: 'views/twitter.html',
        controller: 'TwitterController'
      })
      .when('/mobile', {
        templateUrl: 'views/mobile.html',
        controller: 'MobileController'
      })
      .when('/machine', {
        templateUrl: 'views/machine.html',
        controller: 'MachineController'
      })
      .when('/dimensions', {
        templateUrl: 'views/dimensions.html',
        controller: 'DimensionsController'
      })
      .when('/fraud', {
        templateUrl: 'views/fraud.html',
        controller: 'FraudController'
      })
  });

})();


    
    