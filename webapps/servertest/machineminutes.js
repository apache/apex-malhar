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

// To run these tests Redis server should be running and have data
// (there should be active application that writes to Redis).

var machine = require('../routes/machine');

exports.testExtractMinutesSimple = function (test) {
  test.expect(2);
  var minuteKeys = [
    { day: '7', key: '1800' }
  ];
  var replies = [
    { day: '7'}
  ];
  var response = machine.extractMinutes(minuteKeys, replies);
  test.equal(response.minutes.length, 1);
  test.equal(response.latestKeyWithData, '1800');
  test.done();
};

exports.testExtractMinutesNoRecent = function (test) {
  test.expect(2);
  var minuteKeys = [
    { day: '7', key: '1800'},
    { day: '7', key: '1759'}
  ];
  var replies = [
    null,
    { day: '7'}
  ];
  var response = machine.extractMinutes(minuteKeys, replies);
  test.equal(response.minutes.length, 1);
  test.equal(response.latestKeyWithData, '1759');
  test.done();
};

exports.testExtractMinutesRecentFromPrevDay = function (test) {
  test.expect(2);
  var minuteKeys = [
    { day: '7', key: '1800'},
    { day: '7', key: '1759'}
  ];
  var replies = [
    { day: '6' },
    { day: '7' }
  ];
  var response = machine.extractMinutes(minuteKeys, replies);
  test.equal(response.minutes.length, 1);
  test.equal(response.latestKeyWithData, '1759');
  test.done();
};

exports.testExtractMinutesNoOld = function (test) {
  test.expect(1);
  var minuteKeys = [
    { day: '7'},
    { day: '7'},
    { day: '7'}
  ];
  var replies = [
    { day: '7'},
    { day: '7'},
    null
  ];
  var response = machine.extractMinutes(minuteKeys, replies);
  test.equal(response.minutes.length, 2);
  test.done();
};

exports.testExtractMinutesDayBoundary = function (test) {
  test.expect(1);
  var minuteKeys = [
    { day: '7'},
    { day: '7'},
    { day: '7'}
  ];
  var replies = [
    { day: '7'},
    { day: '7'},
    { day: '6'}
  ];
  var response = machine.extractMinutes(minuteKeys, replies);
  test.equal(response.minutes.length, 2);
  test.done();
};

exports.testExtractMinutes = function (test) {
  test.expect(2);
  var minuteKeys = [
    { day: '7', key: '1800' },
    { day: '7', key: '1759' },
    { day: '6', key: '1758' },
    { day: '6', key: '1757'}
  ];
  var replies = [
    null,
    { day: '7'},
    { day: '6'},
    null
  ];
  var response = machine.extractMinutes(minuteKeys, replies);
  test.equal(response.minutes.length, 2);
  test.equal(response.latestKeyWithData, '1759');
  test.done();
};

exports.testExtractMinutesNoData = function (test) {
  test.expect(2);
  var minuteKeys = [
    { day: '7', key: '1800' },
    { day: '7', key: '1759' }
  ];
  var replies = [
    null,
    null
  ];
  var response = machine.extractMinutes(minuteKeys, replies);
  test.equal(response.minutes.length, 0);
  test.equal(response.latestKeyWithData, null);
  test.done();
};

