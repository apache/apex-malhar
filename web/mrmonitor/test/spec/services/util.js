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

describe('Service: Util', function () {

  // load the service's module
  beforeEach(module('app.service'));

  // instantiate service
  var util;
  var $timeout;

  beforeEach(inject(function (_util_, _$timeout_) {
    util = _util_;
    $timeout = _$timeout_;
  }));

  it('should extract id from application', function () {
    expect(util.extractJobId('application_1385061413075_0824')).toEqual('1385061413075_0824');
  });

  it('should extract id from job', function () {
    expect(util.extractJobId('job_1385061413075_0780')).toEqual('1385061413075_0780');
  });

  it('should delay promise', inject(function ($rootScope, $q) {
    var deferred = $q.defer();

    var resolvedValue;

    util.delay(deferred.promise).then(function (value) {
      resolvedValue = value;
    });

    deferred.resolve('value');
    $rootScope.$apply(); // required to propagate promise resolution

    $timeout.flush();

    expect(resolvedValue).toEqual('value');
  }));

  it('should create minute series', function () {
    var values = [10, 20, 40, 50, 70];
    var now = Date.now();

    var series = util.minuteSeries(values, now);
    expect(series.length).toEqual(values.length);
    expect(series[series.length - 1].timestamp).toEqual(now);
    expect(series[series.length - 1].value).toEqual(values[values.length - 1]);
  });

  it('should create empty minute series', function () {
    var values = [];
    var now = Date.now();

    var series = util.minuteSeries(values, now);
    expect(series.length).toEqual(0);
  });

});
