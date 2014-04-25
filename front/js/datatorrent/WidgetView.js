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
/**
 * Base Widget View
 * 
*/

var _ = require('underscore');
var kt = require('knights-templar');
var BaseView = require('bassview');

// subview
var HeaderView = require('./WidgetHeaderView');

// class definition
var WidgetView = BaseView.extend({
    
    initialize: function(options) {
        // Store the widget definition model
        this.widgetDef = options.widget;
        this.dashDef = options.dashboard;

        // Look for removal handler
        this.onRemove = options.onRemove;

        // Extend events if necessary
        if (this.events !== WidgetView.prototype.events) {
            this.events = _.extend({},WidgetView.prototype.events,this.events);
        }
        
        // Create the subview for the title bar
        this.subview('MAIN_WIDGET_HEADER', new HeaderView({
            model: this.widgetDef,
            collection: this.dashDef.get('widgets')
        }));
        
        // Listen for changes to the width attribute
        this.listenTo(this.widgetDef, 'change:width', this.updateWidth);

        // Listen for changes to the height attribute
        this.listenTo(this.widgetDef, 'change:height', this.updateHeight);
        
        // Listen for removals
        this.listenTo(this.widgetDef, 'remove', this.remove);
        
        // Listen for highlight
        this.listenTo(this.widgetDef, 'hover_on_widget', this._toggleHighlight);
        
        // Set initial width
        this.updateWidth();
        this.updateHeight();
        
        // Listen for changes in width
        this.listenTo(this.widgetDef, 'resize change:width', _.debounce(this.onResize, 300));
    },
    
    // Should be implemented in sub classes
    onResize: function() {
        // empty implementation
    },
    
    // Updates the width of the widget element 
    // when the widgetDef's width property has changed.
    updateWidth: function() {
        var changes = {},
            newWidth = this.widgetDef.get('width')*1;
        
        // if the width is less than 100, it should have 
        if (newWidth <= 80) {
            newWidth -= 0.3;
            changes['margin-right'] = '0.3%';
        } else {
            changes['margin-right'] = '0%';
        }
        
        // adjust real width
        changes.width = newWidth + '%';
        this.$el.css(changes);
        return this;
    },

    updateHeight: function() {
        var newHeight = this.widgetDef.get('height');
        
        if (newHeight !== 'auto') {
            newHeight += 'px';
        }

        this.$el.css('height', newHeight);

        return this;
    },
    
    // Generates a composite id with the
    // dashboard id and widget definition id.
    // It also uses arguments, if supplied.
    compId: function() {
        var parts = [
            this.dashDef.get('dash_id'),
            this.widgetDef.get('id')
        ].concat(Array.prototype.slice.call(arguments));
        return parts.join('.');
    },
    
    // Delegated events
    events: {
        'mousedown .widget-width-resize': '_grabWidthResizer',
        'dblclick .widget-width-resize': 'sizeTo100',
        'mousedown .widget-height-resize': '_grabHeightResizer',
        'dblclick .widget-height-resize': 'sizeToAutoHeight'
    },
    
    // Resizes the widget to 100% of the screen width
    sizeTo100: function(e) {
        this.widgetDef.set('width', 100);
    },

    sizeToAutoHeight: function(e) {
        this.widgetDef.set('height', 'auto');
    },
    
    render: function() {
        var json, html, assignments;
        
        json = {
            widget: this.widgetDef.toJSON(),
            contentClass: this.contentClass,
            content: this.html()
        };

        html = this.BASE_TEMPLATE(json);
        
        this.$el.html(html);

        this._doAssignments();
        
        return this;
    },
    
    // Uses the html() method to re-render the content of the widget
    renderContent: function() {
        this.$('.widget-content').html(this.html());
        this._doAssignments();
        return this;
    },
    
    html: function() {
        var json;
        
        if (this.template) {

            if (this.model) {
                if ( _.isFunction(this.model.serialize) ) {
                    json = this.model.serialize();
                } else {
                    json = this.model.toJSON();
                }
            } else {
                json = {};
            }

            return this.template(json);
        }
        
        return '';
    },
    
    contentClass: '',
    
    assignments: {},
    
    _doAssignments: function() {
        var assignments = _.result(this, 'assignments');
        assignments['.widget-header'] = 'MAIN_WIDGET_HEADER';
        this.assign(assignments);
    },
    
    _grabWidthResizer: function(e) {
        if (e.which !== 1) {
            return;
        }
        e.stopPropagation();
        e.originalEvent.preventDefault();
        
        // Cache actual resize el
        var $resizer = this.$('.widget-width-resize');
        
        // Get the starting horizontal position
        var initX = e.clientX;
        
        // Get the current width of the widget and dashboard
        var initParentWidth = this.$el.parent().width();
        var initWidgetWidth = this.widgetDef.get('width');
        
        // Calculate change and apply new width on mousemove
        var mousemove = (function(e) {
            var curX = e.clientX;
            var change = curX - initX;
            var pChange = Math.round( change*1000 / initParentWidth ) / 10;
            var newWidth = initWidgetWidth*1 + pChange;
            this.widgetDef.set({width:newWidth},{validate:true});
        }).bind(this);
        
        var mouseup = (function(e) {
            $(window).off('mousemove', mousemove);
            $resizer.removeClass('widget-resizing');
        }).bind(this);
        
        $resizer.addClass('widget-resizing');
        
        $(window)
            .on('mousemove', mousemove)
            .one('mouseup', mouseup);
        
    },

    _grabHeightResizer: function(e) {
        if (e.which !== 1) {
            return;
        }
        e.stopPropagation();
        e.originalEvent.preventDefault();

        // Cache actual resize el
        var $resizer = this.$('.widget-height-resize');
        
        // Get the starting horizontal position
        var initY = e.clientY;
        
        // Get the current width of the widget and dashboard
        var initWidgetHeight = this.widgetDef.get('height');
        if (initWidgetHeight === 'auto') {
            initWidgetHeight = this.$el.height();

        }
        
        // Calculate change and apply new height on mousemove
        var mousemove = (function(e) {
            var curY = e.clientY;
            var change = curY - initY;
            var newHeight = Math.round(initWidgetHeight*1 + change);
            this.widgetDef.set({
                height: newHeight
            },{
                validate: true
            });
        }).bind(this);
        
        var mouseup = (function(e) {
            $(window).off('mousemove', mousemove);
            $resizer.removeClass('widget-resizing');
        }).bind(this);
        
        $resizer.addClass('widget-resizing');
        
        $(window)
            .on('mousemove', mousemove)
            .one('mouseup', mouseup);

    },
    
    _toggleHighlight: function(highlighted) {
        this.$el[ highlighted ? 'addClass' : 'removeClass' ]('highlighted');
    },

    remove: function() {
        if (typeof this.onRemove === 'function') {
            this.onRemove();
        }
        BaseView.prototype.remove.apply(this, arguments);
    },
    
    // @final
    BASE_TEMPLATE: kt.make(__dirname+'/WidgetView.html','_')
    
});
exports = module.exports = WidgetView;