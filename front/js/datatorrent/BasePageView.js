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
 * Base Page
 * 
 * This is the base class for all pages. If this.useDashMgr
 * is true, it will generate a sidebar UI for managing
 * widgets and dashboards.
*/
var _ = require('underscore'), Backbone = require('backbone');
var Notifier = require('./Notifier');
var WidgetClassCollection = require('./WidgetClassCollection');
var DashCollection = require('./DashCollection');
var DashMgr = require('./DashMgrView');
var DashTabs = require('./DashMgrView/DashTabs');
var BaseView = require('bassview');
var BasePageView = BaseView.extend({
    
    // This is appended to the localStorage key that 
    // stores state of the page's dashboard configuration.
    // This is to allow for a page module to define itself
    // on initialization. See `./AppInstancePageView.js:initialize()`
    dashExt: '',
    
    initialize: function(options) {
        // scroll to top, TODO save/restore scroll position (so that it is not lost on back button)
        $(document).scrollTop(0);

        // Grab app and dataSource
        this.app = options.app;
        this.dataSource = this.app.dataSource;
        
        // Set up localStorage prefix key
        this.setLocalKey();
        
        // Set up widget class & dash collections
        this.__wClasses = new WidgetClassCollection([]);
        this.__wDashes = new DashCollection([], {
            widgetClasses: this.__wClasses,
            page: this
        });
        
        // Check for dashboardMgr
        if (this.useDashMgr) {
            this.subview('dashMgr', new DashMgr({
                collection: this.__wDashes,
                widgets: this.__wClasses,
                page: this
            }));
            this.subview('dashTabs', new DashTabs({
                collection: this.__wDashes,
                widgets: this.__wClasses,
                page: this
            }));
        }
        
        // Auto-save dashboards
        this.listenTo(this.__wDashes, 'add remove change reset', this.saveDashes);
        
        // Render when selected dashboard has changed
        this.listenTo(this.__wDashes, 'change:selected', this.render);
    },
    
    setLocalKey: function(ext) {
        
        // check name
        if (!this.pageName) {
            throw new TypeError('pageName must be specified on a page');
        }
        
        this.dashExt = ext || this.dashExt;
        this.__lsPrefix = 'datatorrent.pages.'+this.pageName+this.dashExt;
    },
    
    saveDashes: function() {
        // set 'changed' to false
        this.__wDashes.each(function(dash){
            dash.set({'changed': false},{silent: true});
        });
        
        // saving all dashboards (including defaults)
        var json = this.__wDashes.serialize();
        
        // storing in localStorage
        localStorage.setItem(this.__lsPrefix+'.dashboards',JSON.stringify(json));
    },
    
    // Defines the widgets that can be used on this page
    defineWidgets: function(widgetClasses) {
        // Set up the widget class collection
        this.__wClasses.reset(widgetClasses);
    },
    
    // Loads default dashboard and looks up saved dashboards
    // in localStorage, if any.
    loadDashboards: function(defaultDashId, exclude) {
        // start with default dashes
        var dashboards = this.defaultDashes.slice();

        // make sure their 'isDefault' property is true
        _.each(dashboards, function(dash){
            dash.isDefault = true;
        });
        
        // check for default dashes to exclude
        if (exclude && exclude instanceof Array && exclude.length) {
            
            // for every default dash to be excluded, remove it from dashboards
            for (var i = dashboards.length - 1; i >= 0; i--){
                var dash = dashboards[i];
                if (exclude.indexOf(dash.dash_id) > -1) {
                    dashboards.splice(i,1);
                }
            }
            
        }
        
        // look for saved dashboards
        var stored = localStorage.getItem(this.__lsPrefix+'.dashboards');
        if (stored) {
            try {
                
                // to parse the stored value
                var otherDashes = JSON.parse(stored);
                
                // Make sure its an array
                if (otherDashes instanceof Array) {
                    // Check for outdated default dashes to remove
                    var default_ids = _.pluck(dashboards, 'dash_id');
                    otherDashes = _.filter(otherDashes, function(dash){
                        
                        // ensure all widgets are available to instantiate
                        dash.widgets = _.filter(dash.widgets, function(w) {
                            return !! this.__wClasses.get(w.widget);
                        }, this);
                        
                        // if there are no widgets left, remove it
                        if (dash.widgets.length === 0) {
                            return false;
                        }
                        
                        return true;
                        
                    }, this);
                    
                    // Pluck dash_ids from saved
                    var otherDashIds = _.pluck(otherDashes, 'dash_id');

                    // Filter defaults that have custom options in storage
                    dashboards = _.filter(dashboards, function(dash){
                        return otherDashIds.indexOf(dash.dash_id) === -1 ;
                    });

                    // Concat to dashboards
                    dashboards = dashboards.concat(otherDashes);
                    
                } else {
                    
                    throw new Error('invalid localStorage value');
                    
                }
                
            } catch (e) {
                LOG(5, 'Problems loading dashboards', e.stack);
                // clear out bad value
                localStorage.removeItem(this.__lsPrefix+'.dashboards');
            }
        }
        // Reset the dashboard collection
        this.__wDashes.reset(dashboards);
        
        // load last used dash
        var lastDashId;
        var selected = this.__wDashes.find(function(dash){
            return dash.get('selected');
        });
        if (selected) {
            lastDashId = selected.get('dash_id');
        } else {
            lastDashId = defaultDashId;
        }
        this.loadDash(lastDashId, true);
        this.__wDashes.trigger('renderWidgetList');
    },
    
    // Loads a dashboard on the page
    loadDash: function(dash_id, silent) {
        // Check for dash
        var dash = this.__wDashes.get(dash_id);

        if (!dash) {
            
            Notifier.warning({
                'title':'Dashboard not found',
                'text':'The dashboard you selected was not found.'
            });
            
            if (this.__wDashes.length) {
                
                // Select first available dashboard
                var first_dash = this.__wDashes.at(0);
                
                if (first_dash && first_dash.get('dash_id')) {
                    
                    return this.loadDash(first_dash.get('dash_id'));
                    
                }
            }
            
            return;
        }
        
        // Set the selected property
        var selected = this.__wDashes.where({'selected': true});
        if (selected.length) {
            _.each(selected, function(d) {
            d.set({'selected': false},{silent:true});
                this.stopListening(d);
                this.stopListening(d.get('widgets'));
            }, this);
        }
        dash.set({ 'selected': true }, { silent: !!silent });

        // Listen for changes on the dashboard object
        var widgets = dash.get('widgets');
        this.listenTo(widgets, 'remove', function(widget) {
            widget.trigger('remove');
        });
        this.listenTo(widgets, 'add sortWidgets', function() {
            var $parent = this.$('.dashboardMain');
            widgets.each(function(widget) {
                
                var $widget = this.$('.widget[data-id="' + widget.get('id') + '"]');
                
                if ($widget.length) {
                    // widget already there, move it
                    $widget.appendTo($parent);
                    return;
                } else {
                    // create a new element and appent
                    this._renderWidget(widget, dash, $parent);
                    return;
                }
                
            }, this);
        });
        
        return dash;
    },
    
    // Takes an array of widget option objects and instantiates them,
    // appending the views to the provided jQuery element. 
    // Not meant for public use.
    _renderWidgets: function(widgets, curDash, $el) {

        var renderWidget = function(w) {
            this._renderWidget(w, curDash, $el);
        }

        // Render new widgets
        if (widgets instanceof Array) {
            _.each(widgets, renderWidget, this);
        }
        else if (widgets instanceof Backbone.Collection) {
            widgets.each(renderWidget, this);
        }
        
        // allow chaining
        return this;
    },
    
    // Define render method per widget
    _renderWidget: function(w, curDash, $el) {

        var wClassId = w.get('widget');

        var wClass = this.__wClasses.get(wClassId);

        var toInject = {};

        if (wClass === undefined) {
            throw new Error('The widget with name ' + wClassId + ' could not be found in the list of instantiable widgets on page: ' + this.pageName);
        }

        _.each(wClass.get('inject'), function(val, key, list) {
            if (typeof val === 'function') {
                toInject[key] = val.call(this);
            } else {
                toInject[key] = val;
            }
        }, this);
        var View = wClass.get('view');
        var options = _.extend({
            className: 'widget w-' + wClass.get('name'),
            widget: w,
            dashboard: curDash
        }, toInject );

        if (View.prototype.className) {
            options.className += ' ' + View.prototype.className;
        }
        
        var view = new View(options);
        view.listenTo(this, 'clean_up remove_widgets', view.remove);
        view.$el.attr('data-id', w.get('id'));
        view.listenTo(w, 'change:id', function(widget, id) {
            this.$el.attr('data-id', id);
        });
        $el.append( view.render().el );
        
    },
    
    // Default page render method.
    render: function() {
        // Publishes to all previous widget views to clean themselves up
        this.trigger('remove_widgets');
        
        // get selected dash
        var curDash = this.__wDashes.findWhere({'selected': true});

        // get selected dash's widgets
        var widgets = curDash.get('widgets');
        
        // Check for sidebar dashboard manager
        var $el = this.$el;
        
        if (this.useDashMgr) {
            // Reset html
            var dashMgrView = this.subview('dashMgr');
            var marginLeft = dashMgrView.visible ? dashMgrView.width : dashMgrView.collapsedWidth ;
            this.el.innerHTML = 
                '<div class="dashboardMgr" style="width: ' + marginLeft + 'px;"></div>'+
                '<div class="dashboardTabs" style="margin-left: ' + marginLeft + 'px;"></div>' + 
                '<div class="dashboardMain" style="margin-left: ' + marginLeft + 'px;"></div>';
            $el = this.$('.dashboardMain');
            
            // Re-assign the dashboard manager view
            this.assign({'.dashboardMgr':'dashMgr'});
            this.assign({'.dashboardTabs':'dashTabs'});
        }
        
        // render the widgets
        this._renderWidgets(widgets, curDash, $el);
        
        // Make the widgets sortable
        $el.sortable({
            cursor: 'grabbing',
            handle: '.widget-header',
            stop: function() {
                widgets.order = $el.sortable('toArray', { attribute: 'data-id' });
                widgets.sort();
                widgets.trigger('renderList');
            }
        });
        
        // trigger a resize event on all widgets
        curDash.triggerResize();
        return this;
    },
    
    cleanUp: function() {
        this.remove();
    }
    
});
exports = module.exports = BasePageView;