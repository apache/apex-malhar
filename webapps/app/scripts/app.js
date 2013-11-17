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

angular.module('twitter')
    .config(function ($routeProvider) {
        $routeProvider
            .when('/', {
                templateUrl: 'views/twitter.html',
                controller: 'TwitterController'
            })
            .otherwise({
                redirectTo: '/'
            });
    });

angular.module('mobile', ['rest', 'widgets', 'ngGrid', 'google-maps']);

angular.module('mobile')
    .config(function ($routeProvider) {
        $routeProvider
            .when('/', {
                templateUrl: 'views/mobile.html',
                controller: 'MobileController'
            })
            .otherwise({
                redirectTo: '/'
            });
    });

angular.module('machine', ['ng', 'rest', 'widgets']);

angular.module('machine')
    .config(function ($routeProvider) {
        $routeProvider
            .when('/', {
                templateUrl: 'views/machine.html',
                controller: 'MachineController'
            })
            .otherwise({
                redirectTo: '/'
            });
    });

angular.module('dimensions', ['ng', 'rest', 'widgets']);

angular.module('dimensions')
    .config(function ($routeProvider) {
      $routeProvider
        .when('/', {
          templateUrl: 'views/dimensions.html',
          controller: 'DimensionsController'
        })
        .otherwise({
          redirectTo: '/'
        });
    });

angular.module('fraud', ['rest', 'widgets', 'socket']);

angular.module('fraud')
    .config(function($routeProvider) {
        $routeProvider
            .when('/', {
                templateUrl: 'views/fraud.html',
                controller: 'FraudController'
            })
            .otherwise({
                redirectTo: '/'
            });
    });

})();


    
    