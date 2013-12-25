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

describe('Filter: percentage', function () {

  // load the filter's module
  beforeEach(module('app.filter'));

  // initialize a new instance of the filter before each test
  var percentage;
  beforeEach(inject(function ($filter) {
    percentage = $filter('percentage');
  }));

  it('should return percentage', function () {
    var number = 50.12345;
    expect(percentage(number)).toBe('50.12%');
  });

});
