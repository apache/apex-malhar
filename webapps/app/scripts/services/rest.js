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

/*global angular, jQuery, _*/
(function () {
'use strict';

angular.module('rest', ['ng', 'restangular'])
    .factory('rest', ['$q', 'Restangular', function($q, Restangular) {
        return {
            getApp: function (appName) {
                var deferred = $q.defer();
                Restangular.oneUrl('applications', 'ws/v1/applications').get().then(function (response) {
                    var errorMessage = null;
                    if (response && response.apps && response.apps.length > 0) {
                        var apps = _.where(response.apps, { name: appName, state: 'RUNNING' });
                        if (apps.length > 0) {
                            apps = _.sortBy(apps, function (app) { return parseInt(app.elapsedTime, 10); });
                            var app = apps[0];
                            deferred.resolve(app);
                        } else {
                            errorMessage = appName + ' is not found. Please make sure application is running.';
                        }
                    } else {
                        errorMessage = 'No applications available.';
                    }

                    if (errorMessage) {
                        deferred.reject(errorMessage);
                        jQuery.pnotify({
                            title: 'Error',
                            text: errorMessage,
                            type: 'error',
                            icon: false,
                            hide: false
                        });
                    }
                });

                return deferred.promise;
            },

            getMachineData: function (query) {
                var promise = Restangular.one('machine').get(query);

                promise.then(null, function (response) {
                  jQuery.pnotify({
                    title: 'Error',
                    text: 'Error getting data from server. Status Code: ' + response.status,
                    type: 'error',
                    icon: false,
                    hide: false
                  });
                });

                return promise;
            },

          getDimensionsData: function (query) {
            var promise = Restangular.one('dimensions').get(query);

            promise.then(null, function (response) {
              jQuery.pnotify({
                title: 'Error',
                text: 'Error getting data from server. Status Code: ' + response.status,
                type: 'error',
                icon: false,
                hide: false
              });
            });

            return promise;
          }
        };
    }])
    .run(function(Restangular) {
        //Restangular.setBaseUrl('/ws/v1');
        //Restangular.setBaseUrl('/stram/v1');
    });
})();