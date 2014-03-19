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

/*global angular, d3*/
(function () {
'use strict';

angular.module('widgets')
    .directive('widgetsBarChart', function () {
        return {
            restrict: 'A',
            scope: {
                data: "=",
                label: "@",
                onClick: "&"
            },
            link: function(scope, iElement, iAttrs) {
                var svg = d3.select(iElement[0])
                    .append("svg")
                    .attr("width", "100%");

                // on window resize, re-render d3 canvas
                window.onresize = function() {
                    return scope.$apply();
                };
                scope.$watch(function(){
                        return angular.element(window)[0].innerWidth;
                    }, function(){
                        return scope.render(scope.data);
                    }
                );

                // watch for data changes and re-render
                scope.$watch('data', function(newVals, oldVals) {
                    return scope.render(newVals);
                }, true);

                // define render function
                scope.render = function(data){
                    // remove all previous items before render
                    svg.selectAll("*").remove();

                    // setup variables
                    var width, height, max;
                    width = d3.select(iElement[0])[0][0].offsetWidth - 20;
                    // 20 is for margins and can be changed
                    height = scope.data.length * 35;
                    // 35 = 30(bar height) + 5(margin between bars)
                    max = 98;
                    // this can also be found dynamically when the data is not static
                    // max = Math.max.apply(Math, _.map(data, ((val)-> val.count)))

                    // set the height based on the calculations above
                    svg.attr('height', height);

                    //create the rectangles for the bar chart
                    svg.selectAll("rect")
                        .data(data)
                        .enter()
                        .append("rect")
                        .on("click", function(d, i){return scope.onClick({item: d});})
                        .attr("height", 30) // height of each bar
                        .attr("width", 0) // initial width of 0 for transition
                        .attr("x", 10) // half of the 20 side margin specified above
                        .attr("y", function(d, i){
                            return i * 35;
                        }) // height + margin between bars
                        //.transition()
                        //.duration(1000)
                        .attr("width", function(d){
                            var w = d.score/(max/width); // width based on scale
                            return w > 10 ? w : 10;
                        });

                    svg.selectAll("text")
                        .data(data)
                        .enter()
                        .append("text")
                        .attr("fill", "#fff")
                        .attr("y", function(d, i){return i * 35 + 22;})
                        .attr("x", 15)
                        .text(function(d){return d[scope.label];});

                };
            }
        };
    });

})();
