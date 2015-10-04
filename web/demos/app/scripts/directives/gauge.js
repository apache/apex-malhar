/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * Modified copy of https://github.com/lithiumtech/angular_and_d3/blob/master/step5/custom/gauges.js
 */

/*global Gauge, angular, d3*/
(function () {
'use strict';

    angular.module('widgets').directive( 'gauge', function () {
        return {
            restrict: 'A',
            replace: true,
            scope: {
                label: "@",
                min: "=",
                max: "=",
                value: "="
            },
            link: function (scope, element, attrs) {
                var config = {
                    size: 280,
                    label: attrs.label,
                    min: undefined !== scope.min ? scope.min : 0,
                    max: undefined !== scope.max ? scope.max : 100,
                    minorTicks: 5
                };

                var range = config.max - config.min;
                config.yellowZones = [ { from: config.min + range*0.75, to: config.min + range*0.9 } ];
                config.redZones = [ { from: config.min + range*0.9, to: config.max } ];

                scope.gauge = new Gauge( element[0], config );
                scope.gauge.render();
                scope.gauge.redraw( scope.value );

                scope.$watch('value', function() {
                    if (scope.gauge) {
                        scope.gauge.redraw( scope.value );
                    }
                });
            }
        };
    });

})();