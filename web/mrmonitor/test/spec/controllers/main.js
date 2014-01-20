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

describe('Controller: MainCtrl', function () {

  var mockRestService;

  // load the controller's module
  beforeEach(module('app.controller', function ($provide, webSocketProvider) {
    mockRestService = {
      getApp: function () {}
    };

    $provide.constant('settings', { topic: {} });

    $provide.factory('rest', function () {
      return mockRestService;
    });

    $provide.factory('$state', function () {
      return {};
    });

    $provide.factory('$stateParams', function () {
      return {};
    });

    webSocketProvider.setWebSocketObject({});
  }));

  var MainCtrl,
    scope;

  // Initialize the controller and a mock scope
  beforeEach(inject(function ($controller, $q, $rootScope) {
    scope = $rootScope.$new();

    var deferred = $q.defer();
    var promise = deferred.promise;

    spyOn(mockRestService, 'getApp').andReturn(promise);

    MainCtrl = $controller('MainCtrl', {
      $scope: scope
    });
  }));

  it('should be defined', function () {
    expect(MainCtrl).toBeDefined();
    //TODO
    //expect(mockRestService.getApp).toHaveBeenCalled();
  });
});
