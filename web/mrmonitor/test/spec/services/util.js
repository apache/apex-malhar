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
  beforeEach(inject(function (_util_) {
    util = _util_;
  }));

  it('should extract id from application', function () {
    expect(util.extractJobId('application_1385061413075_0824')).toEqual('1385061413075_0824');
  });

  it('should extract id from job', function () {
    expect(util.extractJobId('job_1385061413075_0780')).toEqual('1385061413075_0780');
  });

});
