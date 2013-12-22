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
var _ = require('underscore');
var Backbone = require('backbone');
var d3 = require('d3');
var LiveChartView = Backbone.View.extend({
    
    initialize: function(options) {
        // Create flags for the no-data-text and no-series element  (see this.view.update)
        this.no_data_text = false;
        
        // Listen to data updates
        var data = this.model.get('data');
        var plots = this.model.plots;
        this.listenTo(data, 'add', this.update);
        this.listenTo(plots, 'change:visible', this._visiblePlotChange);
        this.listenTo(this.model, 'change:width change:height', this._dimensionChange);
        this.listenTo(this.model, 'change:time_range', this._timeRangeChange);
        
        // Set number formatter for overlays
        this.overlay_formatter = d3.format('n');

        // Set an id for clip-path element:
        this.clip_path_id = 'clip-' + this.cid;
    },
    
    render: function() {
        var options = this.model.toJSON();
        this.$el.html('<div class="livechart-container"></div>');
        this._setSVG(options);
        this._setScale(options);
        this._setAxes(options);
        this._setLineFn(options);
        this._setSeries(options);
        return this;
    },
    
    update: function(data) {
        
        // Alias repeated-use variables
        var options = this.model.toJSON(),
            time_key = options.time_key,
            duration = options.duration,
            margin = options.margin,
            width = options.width,
            height = options.height,
            y_guides = options.y_guides,
            paused = options.paused,
            min_y_range = options.min_y_range,
            domain_padding = options.domain_padding,
            mouse_over = options.mouse_over,
            x = this.x,
            y = this.y,
            line = this.line,
            seriesGroups = this._seriesGroups,
            xAxisGroup = this._xAxisGroup,
            yAxisGroup = this._yAxisGroup,
            yGuideGroup = this._yGuideGroup,
            series,
            no_render_lines = false,
            seriesLines,
            old_y_domain,
            max_time,
            new_x_domain,
            new_y_domain,
            x_axis_movement,
            changed_y_domain,
            newGroups;
        
        // Can accept a new data set
        data = this.model.get('data').toJSON();
        
        // Check if paused
        if (paused) {
            return;
        }
        
        // Check if data is there to draw
        if (data.length === 0) {
    
            // Add "no data" text
            this.no_data_text = this.svg.append('text')
                .attr('class', 'no-data-text')
                .text('No data provided')
                .attr('x', function() { return width / 2; })
                .attr('y', function() { return height / 2; });
    
            return;
    
        } else if (this.no_data_text) {
    
            // remove any no-data-text
            this.no_data_text.remove();
            this.no_data_text = false;
    
        }

        // Check if there are series specified
        if (!this.model.plots.length) {
            throw new Error('The chart requires at least one series to be specified.');
        }
        
        series = this._setSeries(options);
        
        // Check if all plots are not visible
        if (this.model.plots.every(function(plot) {
            return !plot.get('visible');
        })) {
            no_render_lines = true;
        }

        // Calculate new domains and x-axis movement
        old_y_domain = y.domain();
        max_time = d3.max(data, function(d) { return d[time_key]; });
        new_x_domain = [max_time - options.time_range, max_time];
        new_y_domain = no_render_lines ? [0,0] : [
            d3.min(series, function(c) { return d3.min(c.values, function(v) { return v.value; }); }),
            d3.max(series, function(c) { return d3.max(c.values, function(v) { return v.value; }); })
        ];
        domain_padding *= (new_y_domain[1] - new_y_domain[0]);
        new_y_domain[0] -= domain_padding;
        new_y_domain[1] += domain_padding;
        
        // check min_y_range
        if ( (new_y_domain[1] - new_y_domain[0]) < min_y_range) {
            var more_padding = (min_y_range - (new_y_domain[1] - new_y_domain[0])) / 2;
            new_y_domain[0] -= more_padding;
            new_y_domain[1] += more_padding;
        }
        
        x_axis_movement = this._last_max_time
            ? x(this._last_max_time) - x(max_time)
            : 0;
            
        changed_y_domain = old_y_domain[0] != new_y_domain[0] || old_y_domain[1] != new_y_domain[1];
        
        // Set the last_max_time for future renders
        this._last_max_time = max_time;
        
        // Set the new x domain
        x.domain(new_x_domain);

        // redraw the lines
        seriesLines = seriesGroups.select('path')
            .attr('transform', 'translate(' + -x_axis_movement + ') ')
            .attr('d', function(d) {
                return line(d.values);
            });
        
        // Set animating attr on model
        this.model.animating = true;
        
        // slide the x-axis
        xAxisGroup.transition().ease(options.easing).duration(duration).call(x.axis);

        // slide domain if changing
        if (changed_y_domain) {
            y.domain(new_y_domain);
            seriesLines
                .transition().ease(options.easing).duration(duration)
                .attr('transform', 'translate(0)')
                .attr('d', function(d) {
                    return line(d.values);
                });
        } else {
            // slide the path
            seriesLines
                .transition().ease(options.easing).duration(duration)
                .attr('transform', 'translate(0)');
        }

        // slide the y-axis
        yAxisGroup
            .transition()
            .ease(options.easing)
            .duration(duration)
            .each("end", _.bind(function() {
                this.model.animating = false;
            }, this))
            .call(y.axis)
            

        
        
        if (y_guides) {
            
            // create tick marks
            var ticks = y.ticks(4).map(function(tick) {
                return y(tick);
            });
            var guides = yGuideGroup.selectAll('.guideline')
                .data(ticks);
            
            // transition the current guides
            guides.transition()
                .ease(options.easing)
                .duration(duration)
                .attr('y1', function(d) {
                    return d;
                })
                .attr('y2', function(d) {
                    return d;
                });
                
            // add new guides
            guides.enter()
                .append('line')
                .attr({
                    'class': 'guideline',
                    'x1': 1,
                    'x2': width,
                    'y1': 0,
                    'y2': 0,
                    'opacity': 1
                })
                .transition()
                .ease(options.easing)
                .duration(duration)
                .attr('y1', function(d) {
                    return d;
                })
                .attr('y2', function(d) {
                    return d;
                });

            // remove old guides
            guides.exit()
                .transition()
                .ease(options.easing)
                .duration(duration)
                .attr({
                    'y1': 0,
                    'y2': 0,
                    'opacity': 0
                })
                .remove();
        }
        
        if (mouse_over) {
            this.svg.select('.overlay-trg g.overlays').selectAll('.series-overlay')
                .transition()
                .duration(duration)
                .attr('transform', function(d) {
                    var pt = this.curPoint;
                    if (!pt) return '';
                    return "translate(" + x(pt.time) + "," + y(pt.value) + ")";
                })
        }
        
        return this;

    },
    
    events: {
        'mousedown .chart-resizer': '_grabResizer',
        'dblclick .chart-resizer': 'resizeChartToCtnr'
    },
    
    resizeChartToCtnr: function() {
        var margin = this.model.get('margin');
        var newWidth = this.$el.width() - margin.left - margin.right;
        this.model.set('width', newWidth);
    },
    
    // Set the svg element
    _setSVG: function(options) {

        // Alias options and margins
        var options = options || this.model.toJSON(),
            margin = options.margin,
            width = options.width, 
            height = options.height,
            outerWidth = width + margin.left + margin.right,
            outerHeight = height + margin.top + margin.bottom,
            svg,
            resizer,
            selection,
            clipPath,
            resizerSize = 17;
            
        // Get the d3 selection
        selection = d3.select(this.$('.livechart-container')[0]);

        // Check that this is a valid selection
        if (!selection) {
            throw new TypeError('A DOM element or query selector string must be provided as the "el" key in options');
        }
        
        // Set the outer height
        this.$el.css('height', (outerHeight + 5) + 'px');

        // Create or update the svg
        svg = selection.select('.live-chart-svg');
        if (svg.empty()) {
            svg = selection.append('svg');
        }
        svg
            .attr('class', 'live-chart-svg')
            .attr('width', outerWidth)
            .attr('height', outerHeight);
        
        // Create group object for everything else
        this.svg = svg.select('.live-chart-g');
        if (this.svg.empty()) {
            this.svg = svg = svg.append('g').attr('class', 'live-chart-g');
        }
        else {
            svg = this.svg;
        }
        svg.attr("transform", "translate(" + margin.left + "," + margin.top + ")");

        // The clipping path
        clipPathRect = svg.select('defs #' + this.clip_path_id + ' rect');
        if (clipPathRect.empty()) {
            clipPathRect = svg
                .append('defs')
                .append('clipPath')
                .attr('id', this.clip_path_id)
                .append('rect');
        }
        clipPathRect
            .attr('width', width)
            .attr('height', height);
        
        // The resizer
        resizer = svg.select('.chart-resizer');
        if (resizer.empty()) {
            resizer = svg.append('rect').attr('class', 'chart-resizer');
        }
        resizer.attr({
                'width': resizerSize,
                'height': resizerSize,
                'x': width - resizerSize,
                'y': height - resizerSize
            });  
        
        return this;
    
    },
    
    // Set the scale functions
    _setScale:function(options) {

        // Alias to options
        var options = options || this.model.toJSON();

        // The scale functions
        if (this.x && this.y) {
            this.x.range([0, options.width]);
            this.y.range([options.height, 0]);
        }
        else {
            this.x = d3.time.scale().range([ 0, options.width ]),
            this.y = d3.scale.linear().range([ options.height, 0 ]);
        }

    },

    // Creates the line function
    _setLineFn:function(options) {

        var options = options ||this.model.toJSON(),
            x = this.x,
            y = this.y,
            interpolation = options.interpolation;

        // The line generator,
        // creates the d attribute for each path
        this.line = d3.svg.line().interpolate(interpolation)
            // define the x and y coords
            .x(function(d) {
                return x(d.time);
            })
            .y(function(d) {
                return y(d.value);
            });
    },

    // Sets up the axes objects and svg elements
    _setAxes:function(options) {

        var x = this.x, y = this.y, height = this.model.get('height'), svg = this.svg, format = d3.format(this.model.get('y_label_format'));

        // create axes if not there
        x.axis = d3.svg.axis().scale(x).orient('bottom');
        y.axis = d3.svg.axis().scale(y).orient('left');
        
        if (format) {
            y.axis.tickFormat(format);
        }
        

        // create the x- and y-axis svg objects
        this._xAxisGroup = svg.append("g")
            .attr("class", "x axis")
            .attr("transform", "translate(0," + height + ")")
            .call(x.axis);
        this._yAxisGroup = svg.append("g")
            .attr("class", "y axis")
            .call(y.axis);

        // create the guides
        this._yGuideGroup = svg.append("g")
            .attr("class", "yGuides");
    
    },
    
    // Transitions axis views based on axis models.
    _updateAxes: function() {
        yAxisGroup.transition().ease(options.easing).duration(duration).call(y.axis);
    },

    // Creates the svg groups and paths for each series
    _setSeries:function(options) {

        var options = options ||this.model.toJSON(),
            time_key = options.time_key,
            line = this.line,
            data = this.model.generateSeriesData() || [],
            keyFn = function(data) {
                return data.key;
            },
            groups,
            duration = this.model.get('duration');
        
        // Set up the series g elements
        this._seriesGroups = this.svg.selectAll('.seriesGroup').data(data, keyFn);
        
        // remove exiting elements
        this._seriesGroups
            .exit()
            .transition()
            .duration(duration)
            .attr('opacity', 0)
            .remove()
        ;
        
        // add new elements
        groups = this._seriesGroups
            .enter()
            .insert('g','g.overlay-trg')
            .attr('class', 'seriesGroup')
            .attr("clip-path", "url(#" + this.clip_path_id + ")")
            .attr('opacity', 0)
        ;
        
        // add paths
        groups
            .append('path')
            .attr('class', function(d) {
                return 'seriesLine series-key-' + d.key;
            })
            .attr('stroke', function(d){
                return d.plot.color;
            })
            .attr('d', function(d) {
                return line(d.values);
            })
        ;
        
        // transition in the path groups
        groups
            .transition()
            .duration(duration)
            .attr('opacity', 1);
        
        this._seriesLines = this._seriesGroups.select('.seriesLines');
        
        // mouse overlays
        if (options.mouse_over) {
            
            // Get the group that holds all elements involved in overlays
            var overlay_target = this.svg.select('g.overlay-trg');
            // Holds a selection of overlay groups; for use as closure in event handler
            var overlays;
            // Store this for use in event handlers
            var self = this;
            // Create a bisecting function for choosing which point to overlay
            var bisectDate = d3.bisector(function(d) { return d.time; }).left;
            // Check if it has been created
            if (overlay_target.empty()) {
                // Create it
                overlay_target = this.svg
                    .append('g')
                    .attr('class', 'overlay-trg')
                    .attr("clip-path", "url(#" + this.clip_path_id + ")");
                    
                // Create the group that will hold the series-specific overlay items
                overlay_target.append('g')
                    .attr('class', 'overlays')
                    .style('display', 'none')
                
                // Create the vertical cursor line
                overlay_target.append('line')
                    .style('display', 'none')
                    .attr('class', 'cursor-line')
                    .attr('y1', 0)
                    .attr('y2', options.height)
                    .attr('x1', 0)
                    .attr('x2', 0);
                
                // Create the rect that will listen to the mouse
                overlay_target.append('rect')
                    // Give it a class
                    .attr('class','mouseover-rect')

                    // Set the listeners
                    .on('mouseover', function() {
                        overlay_target.select('g.overlays')
                            .style('display', null);
                        
                        overlay_target.select('.cursor-line')
                            .style('display', null);
                            
                    })
                    .on('mousemove', function() {
                        
                        // Get the x-value for the current mouse position
                        var xPos = d3.mouse(this)[0];
                        
                        // Update position of cursor
                        self.svg.select('.overlay-trg .cursor-line')
                            .attr('x1', xPos)
                            .attr('x2', xPos);
                        
                        // Check if this should not be rendered
                        if (self.model.animating === true) {
                            return;
                        }
                        
                        // Select all of the series overlays
                        overlays = self.svg.selectAll('.overlay-trg g.overlays .series-overlay');
                        
                        // Convert current position of cursor
                        var x0 = self.x.invert(xPos);
                        
                        // Get the points for each series (key)
                        var overlay_points = {};
                        overlays.each(function(d) {
                            var i  = bisectDate(d.values, x0, 1);
                            var data = d.values,
                                d0 = data[i - 1],
                                d1 = data[i];
                                if (d0 === undefined || d1 === undefined) return;
                            this.curPoint = overlay_points[d.key] = x0 - d0.time > d1.time - x0 ? d1 : d0;
                        });
                        // Move the point to the right spot and update the text
                        overlays
                            .attr('transform', function(d) {
                                var d = overlay_points[d.key];
                                if (!d) return '';
                                return "translate(" + self.x(d.time) + "," + self.y(d.value) + ")";
                            })
                            .select('text')
                                .text(function(d) {
                                    var d = overlay_points[d.key];
                                    if (!d) return '';
                                    return self.overlay_formatter(d.value);
                                });

                        overlays
                            .select('rect')
                            .each(function(d) {
                                var bb = this.parentElement.querySelector('text').getBBox();
                                var x_translate = -1 * bb.width / 2 - 2;
                                var y_translate = -1 * (bb.height + 5);
                                this.setAttribute('width', bb.width + 4);
                                this.setAttribute('height', bb.height);
                                this.setAttribute('transform','translate(' + x_translate + ', ' + y_translate + ')');
                            });
                    })
                    .on('mouseout', function() {
                        overlay_points = {};
                        overlay_target.select('g.overlays')
                            .style('display', 'none');
                        overlay_target.select('.cursor-line')
                            .style('display', 'none')
                    });
            }
            
            // Update the height and width of the rect
            overlay_target.select('rect.mouseover-rect')
                .attr('width', options.width)
                .attr('height', options.height);
                
            // Set the new height of the cursor line
            overlay_target.select('line.cursor-line')
                .attr('y2', options.height);
            
            // Set the data to the overlays
            overlays = this.svg.select('.overlay-trg g.overlays')
                .selectAll('.series-overlay')
                .data(data, keyFn);
            
            // For new ones, add a group with a circle and text element
            var new_overlays = overlays.enter()
                .append('g')
                .attr('class', function(d) {
                    return 'series-overlay series-key-' + d.key;
                });

            // add circle element
            new_overlays
                .append('circle')
                .attr('r', 4.5)
                .style('fill', function(d) {
                    return d.plot.color;
                });
            
            // add rectangle before text
            new_overlays
                .append('rect')
                .attr('class', 'overlay-background')
                .attr('transform', "translate(0,-8)")
                .attr('rx', 2)
                .attr('ry', 2);

            // add text last to be on top
            new_overlays
                .append('text')
                .attr('class', 'overlay-text')
                .attr('text-anchor','middle')
                .attr('transform', "translate(0,-8)");

            
            
            // Remove old ones   
            overlays.exit().remove();
        }
        
        return data;
    },
    
    _visiblePlotChange:function() {
        // pause
        // this.model.set('paused', true);
        this.update();
        this.model.set('paused', true);
        setTimeout(_.bind(function() {
            this.model.set('paused', false);
        }, this), this.model.get('duration'))
    },
    
    _dimensionChange:function() {
        // set svg dimensions
        var options = this.model.toJSON();
        if (this._lastHeightUpdate === options.height && this._lastWidthUpdate === options.width) {
            return;
        } 
        this._setSVG(options);
        this._setScale(options);
        this._xAxisGroup.attr('transform', 'translate(0,' + options.height + ')');
        this.update();
        if (options.y_guides) {
            this._yGuideGroup.selectAll('.guideline').attr('x2', options.width);
        }
        
        // store these dimensions for checking against future changes
        this._lastHeightUpdate = options.height;
        this._lastWidthUpdate = options.width;
    },
    
    _timeRangeChange: function() {
        this.update();
    },
    
    _grabResizer: function(evt) {
        if ( evt.button === 2 ) return;
        evt.preventDefault();
        evt.originalEvent.preventDefault();
        
        // Set up initial mouse position
        var initialX = evt.clientX;
        var initialY = evt.clientY;
        var initialWidth = this.model.get('width');
        var initialHeight = this.model.get('height');
        var marginLeft = this.model.get('margin').left;
        
        // Set up marquee object for resizing
        var vpOffset = this.$('.live-chart-svg').offset();
        var $marq = $('<div class="chart-resize-marquee"></div>')
            .css({
                left: vpOffset.left + marginLeft,
                top: vpOffset.top
            })
            .appendTo($('body'));
        
        // Calculates new dimensions at current mouse position
        function getNewDims(evt) {
            evt.preventDefault();
            evt.originalEvent.preventDefault();
            var deltaX = evt.clientX - initialX;
            var deltaY = evt.clientY - initialY;
            var newWidth = +initialWidth + +deltaX;
            var newHeight = +initialHeight + +deltaY;
            return {
                width: newWidth,
                height: newHeight
            };
        }
        
        // When the mouse is released
        var release = function release(evt) {
            var dims = getNewDims(evt);
            this.model.set('width',dims.width);
            this.model.set('height',dims.height);
            $(window).off('mousemove', resize);
            $marq.remove();
        }.bind(this);
        
        function resize(evt) {
            var dims = getNewDims(evt);
            dims.width += 'px';
            dims.height += 'px';
            $marq.css(dims);
        }
        
        // $(window).one('mouseup', release);
        // $(window).on('mousemove', resize);
        $(window).on('mousemove', resize);
        $(window).one('mouseup', release);
    }
});

exports = module.exports = LiveChartView;