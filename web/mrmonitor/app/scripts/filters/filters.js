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

angular.module('app.filter')
  .filter('percentage', function ($filter) {
    var numberFilter = $filter('number');

    return function (input) {
      if (_.isNumber(input) && (input >= 0) && (input <= 100)) {
        return numberFilter(input, 2) + '%';
      } else {
        return null;
      }
    };
  })
  .filter('appId', function () {
    return function (input) {
      if (input) {
        return input.replace('application_', 'job_');
      } else {
        return null;
      }
    };
  })
  .filter('elapsed', function() {
    return function(timeStamp) {
      var options = { timeChunk: timeStamp * 1, unixUptime: true };

      _.defaults(options, {
        compareDate: +new Date(),
        timeChunk: undefined,
        maxUnit: 'year',
        unixUptime: false,
        maxLevels: 3,
        timeStamp: timeStamp || 0
      });
      var remaining = (options.timeChunk !== undefined) ? options.timeChunk : options.compareDate - options.timeStamp;
      var string = '';
      var separator = ', ';
      var level = 0;
      var maxLevels = options.maxLevels;
      var milliPerSecond = 1000;
      var milliPerMinute = milliPerSecond * 60;
      var milliPerHour = milliPerMinute * 60;
      var milliPerDay = milliPerHour * 24;
      var milliPerWeek = milliPerDay * 7;
      var milliPerMonth = milliPerWeek * 4;
      var milliPerYear = milliPerDay * 365;

      if (options.unixUptime) {
        var days = Math.floor(remaining / milliPerDay);
        remaining -= days*milliPerDay;
        var hours = Math.floor(remaining / milliPerHour);
        remaining -= hours*milliPerHour;
        var minutes = Math.round(remaining / milliPerMinute);

        if (days === 0) {
          minutes = Math.floor(remaining / milliPerMinute);
          remaining -= minutes*milliPerMinute;
          var seconds = Math.round(remaining / 1000);
          string = (hours < 10 ? '0' : '')+hours+':'+(minutes < 10 ? '0' : '')+minutes+':'+(seconds < 10 ? '0' : '')+seconds;
        }
        else {
          string = days + ' days, ' + hours.toString() + ':' + (minutes < 10 ? '0' : '') + minutes.toString();
        }

      } else {
        var levels = [
          { plural: 'years', singular: 'year', ms: milliPerYear },
          { plural: 'months', singular: 'month', ms: milliPerMonth },
          { plural: 'weeks', singular: 'week', ms: milliPerWeek },
          { plural: 'days', singular: 'day', ms: milliPerDay },
          { plural: 'hours', singular: 'hour', ms: milliPerHour },
          { plural: 'minutes', singular: 'minute', ms: milliPerMinute },
          { plural: 'seconds', singular: 'second', ms: milliPerSecond }
        ];

        var crossedThreshold = false;
        for (var i=0; i < levels.length; i++) {
          if ( options.maxUnit === levels[i].singular ) {
            crossedThreshold = true;
          }
          if ( remaining < levels[i].ms || !crossedThreshold ) {
            continue;
          }
          level++;
          var num = Math.floor( remaining / levels[i].ms );
          var label = num === 1 ? levels[i].singular : levels[i].plural ;
          string += num + ' ' + label + separator;
          remaining %= levels[i].ms;
          if ( level >= maxLevels ) {
            break;
          }
        }
        string = string.substring(0, string.length - separator.length);
      }


      return string;
    };
  });
