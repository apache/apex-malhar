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

/*global angular, _*/
(function () {
'use strict';

angular.module('widgets').filter('elapsed', function() {
    return function(timeStamp) {
        var options = { timeChunk: timeStamp * 1, unixUptime: true };

        _.defaults(options, {
            compareDate: +new Date(),
            timeChunk: undefined,
            maxUnit: "year",
            unixUptime: false,
            max_levels: 3,
            timeStamp: timeStamp || 0
        });
        var remaining = (options.timeChunk !== undefined) ? options.timeChunk : options.compareDate - options.timeStamp;
        var string = "";
        var separator = ", ";
        var level = 0;
        var max_levels = options.max_levels;
        var milli_per_second = 1000;
        var milli_per_minute = milli_per_second * 60;
        var milli_per_hour = milli_per_minute * 60;
        var milli_per_day = milli_per_hour * 24;
        var milli_per_week = milli_per_day * 7;
        var milli_per_month = milli_per_week * 4;
        var milli_per_year = milli_per_day * 365;

        if (options.unixUptime) {
            var days = Math.floor(remaining / milli_per_day);
            remaining -= days*milli_per_day;
            var hours = Math.floor(remaining / milli_per_hour);
            remaining -= hours*milli_per_hour;
            var minutes = Math.round(remaining / milli_per_minute);

            if (days === 0) {
                minutes = Math.floor(remaining / milli_per_minute);
                remaining -= minutes*milli_per_minute;
                var seconds = Math.round(remaining / 1000);
                string = (hours < 10 ? "0" : "")+hours+':'+(minutes < 10 ? "0" : "")+minutes+':'+(seconds < 10 ? "0" : "")+seconds;
            }
            else {
                string = days + " days, " + hours.toString() + ":" + (minutes < 10 ? "0" : "") + minutes.toString();
            }

        } else {
            var levels = [
                { plural: "years", singular: "year", ms: milli_per_year },
                { plural: "months", singular: "month", ms: milli_per_month },
                { plural: "weeks", singular: "week", ms: milli_per_week },
                { plural: "days", singular: "day", ms: milli_per_day },
                { plural: "hours", singular: "hour", ms: milli_per_hour },
                { plural: "minutes", singular: "minute", ms: milli_per_minute },
                { plural: "seconds", singular: "second", ms: milli_per_second }
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
                string += num + " " + label + separator;
                remaining %= levels[i].ms;
                if ( level >= max_levels ) {
                    break;
                }
            }
            string = string.substring(0, string.length - separator.length);
        }


        return string;
    };
});

})();


    
    