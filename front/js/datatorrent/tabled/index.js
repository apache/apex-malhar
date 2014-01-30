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
var _ = require('underscore'), Backbone = require('backbone');
var BaseView = require('bassview');
var Column = require('./lib/Column').model;
var Columns = require('./lib/Column').collection;
var Thead = require('./lib/Thead');
var Tbody = require('./lib/Tbody');
var Scroller = require('./lib/Scroller');

var ConfigModel = Backbone.Model.extend({
    
    defaults: {
        // Makes table width and column widths adjustable
        adjustable_width: true,
        // Save the state of the table widths
        save_state: false,
        // Default minimum column width, in pixels
        min_column_width: 30,
        // Default width for the table itself. Either pixels or 'auto'
        table_width: 'auto',
        // Default max number of rows to render before scroll on tbody
        max_rows: 30,
        // Default offset for the tbody
        offset: 0,
        // Set in the rendering phase of the tbody (code smell...)
        total_rows: 0
    },
    
    validate: function(attrs) {
        if ( attrs.offset > Math.max(attrs.max_rows, attrs.total_rows) - attrs.max_rows ) {
            return "Offset cannot be that high.";
        }
        if (attrs.offset < 0) {
            return "Offset must be greater than 0";
        }
        if (attrs.max_rows < 1) {
            return "max_rows must at least be 1";
        }
    },
    getVisibleRows: function() {
        var total = 0;
        var offset = this.get('offset');
        var limit = this.get('max_rows');
        var rows_to_render = [];
        this.get("collection").each(function(row, i){
            
            if ( this.passesFilters(row) ) ++total;
            else return;
            
            if ( total <= offset ) return;
            
            if ( total > (offset + limit) ) return;
            
            rows_to_render.push(row);
            
        }, this);
        
        var prev_total = this.get('total_rows')*1;
        if (total !== prev_total) {
            this.set('total_rows', total)
            var newOffset = Math.max(0, Math.min(total - limit, offset));
            if (newOffset === offset) this.trigger('update');
            else this.set('offset', newOffset);
            
            return false;
        }
        
        return rows_to_render;
    },
    passesFilters: function(row){
        return this.columns.every(function(column){
            if (column.get('filter_value') == "" || typeof column.get('filter') !== "function") return true;
            return this.passesFilter(row, column);
        }, this);
    },
    passesFilter: function(row, column){
        return column.get('filter')( column.get('filter_value'), row.get(column.get('key')), column.getFormatted(row), row );
    },
    
});

var Tabled = BaseView.extend({
    
    initialize: function(options) {

        // Ensure that this.collection (the data) is a backbone collection
        // if ( !(this.collection instanceof Backbone.Collection) ) throw new Error("Tabled must be provided with a backbone collection as its data");
        
        // Config object
        this.config = new ConfigModel(options);

        // Columns
        this.columns = new Columns(this.config.get("columns"),{config: this.config});
        this.config.columns = this.columns;
        
        // Subviews
        this.subview("thead", new Thead({
            collection: this.columns,
            config: this.config
        }));
        this.subview("tbody", new Tbody({
            collection: this.collection,
            columns: this.columns
        }));
        this.subview('scroller', new Scroller({
            model: this.config,
            tbody: this.subview('tbody')
        }));
        
        // State
        if (this.config.get("save_state")) {
            this.restorePreviousState();
        }
        
        // Listeners
        this.listenTo(this.columns, "change:width", this.onWidthChange );
        this.listenTo(this.columns, "change:filter_value", function(model, attr){
            this.config.set("offset", 0);
            this.renderBody();
            var column_filters = this.columns.map(function(col) {
                return { id: col.get('id'), filter_value: col.get('filter_value')};
            });
            this.state('column_filters', column_filters);
        });
        this.listenTo(this.config, "change:max_rows", this.onMaxRowChange);
        this.listenTo(this.columns, "change:comparator", this.updateComparator);
        this.listenTo(this.columns, "sort", this.onColumnSort);
        
        // HACK: set up data comparator
        this.columns.updateComparator();
    },
    
    template: [
        '<div class="tabled-ctnr"><div class="tabled-inner">',
        '<div class="tabled">',
        '<div class="thead"></div>',
        '<div class="tbody-outer">',
        '<div class="tbody"></div>',
        '<div class="scroller"></div>',
        '</div>',
        '<div class="resize-table">',
        '<div class="resize-grip"></div><div class="resize-grip"></div><div class="resize-grip"></div>',
        '</div>',
        '</div>',
        '</div></div>'
    ].join(""),
    
    render: function() {
        this.lockHeight();
        // Set initial markup
        this.$el.html(this.template);
        // Set the widths of the columns
        this.setWidths();
        // (Re)render subviews
        this.assign({
            '.thead': 'thead',
            '.tbody': 'tbody',
            '.scroller': 'scroller'
        });
        this.unlockHeight();
        
        return this;
    },
    
    lockHeight: function() {
        // Lock height to prevent scrollbar craziness
        this.$el.css({'height':this.$el.height()+'px'});
    },
    
    unlockHeight: function() {
        this.$el.css({'height':'auto'});
    },
    
    renderBody: function(){
        this.assign({
            '.tbody': 'tbody',
            '.scroller': 'scroller'
        });
    },
    
    renderHead: function(){
        this.assign({
            '.thead': 'thead'
        });
    },
    
    onWidthChange: function(model, value, options){
        this.adjustInnerDiv();
        
        // Save the widths
        if (!this.config.get("save_state")) return;
        if (options.save_state === false) return;
        var widths = this.columns.reduce(function(memo, column, key){
            memo[column.get('id')] = column.get('width');
            return memo;
        }, {}, this);
        this.state('column_widths', widths);
    },
    
    onMaxRowChange: function(model, value) {
        this.renderBody();
        this.state('max_rows', value);
    },
    
    onColumnSort: function() {
        this.render();
        
        // Save sort
        if (!this.config.get("save_state")) return;
        var sorts = this.columns.col_sorts;
        this.state('column_sorts', sorts);
    },
    
    setWidths: function() {
        
        // Table's width
        var totalWidth = this.config.get("table_width") === 'auto' ? this.$el.width() : this.config.get("table_width");

        // Columns to make the default width
        var makeDefault = [];
        // This is calculated as widths are set
        var adjustedWidth = 0;

        // Check the columns
        this.columns.each(function(column, key){

            // Check for preset width
            var col_width = column.get('width');
            // Check for minimum column width
            // var min_col_width = column.get('min_column_width')
            // Set to width if 
            if ( col_width ) {
                // reduce the totalWidth by the preset width amount
                totalWidth -= col_width;
                // add to adjusted width
                adjustedWidth += col_width;
            }
            else {
                makeDefault.push(column);
            }
        });

        // Get the average width after preset widths are considered
        var avg_width = makeDefault.length ? totalWidth/makeDefault.length : 0 ;
        // Calculate the default width for colums without presets
        var defaultWidth = Math.max(Math.floor(avg_width), this.config.get("min_column_width")) ;
        // Set the widths 
        makeDefault.forEach(function(column, key){
            var width = Math.max(defaultWidth, column.get('min_column_width') || defaultWidth);
            column.set({'width': width},{save_state:false});
            adjustedWidth += width;
        });
        this.$('.tabled-inner').width(adjustedWidth);
    },
    
    adjustInnerDiv: function() {
        var width = this.columns.reduce(function(memo, column){
            var width = column.get('width') || column.get('min_column_width');
            return memo*1 + width*1;
        }, 0);
        this.$('.tabled-inner').width(width+1); // +1 for firefox!
    },
    
    events: function() {
        var default_events = {
            'mousedown .resize-table': 'grabTableResizer',
            'dblclick .resize-table':  'resizeTableToCtnr'
        }
            
        // Look for interactions provided by columns
        var interactions = _.filter(this.columns.pluck('interaction'), function(ixn) { return ixn !== undefined; });
        _.extend.apply(default_events, [default_events].concat(interactions));
        
        return default_events; 
    },
    
    grabTableResizer: function(evt){
        evt.preventDefault();
        evt.stopPropagation();

        // check for right click
        if (evt.which !== 1) {
            return;
        }

        var self = this;
        
        // Horizontal
        var mouseX = evt.clientX;
        var resizableColCount = 0;
        var col_state = this.columns.reduce(function(memo, column, index){
            memo[column.get('id')] = column.get('width');
            if (!column.get('lock_width')) ++resizableColCount;
            return memo;
        },{},this);
        
        // Vertical 
        var mouseY = evt.clientY;
        var row_height = $(".tr", this.$el).height();
        var initMax = Math.max(1, Math.min(this.config.get('max_rows'), this.collection.length));
        
        var table_resize = function(evt){
            // Horizontal
            var changeX = (evt.clientX - mouseX)/resizableColCount;
            self.columns.each(function(column){
                column.set({
                    "width": col_state[column.get("id")]*1 + changeX
                }, { validate:true });
            });
            
            // Vertical
            var changeY = (evt.clientY - mouseY);
            var abChangeY = Math.abs(changeY);
            if ( abChangeY > row_height) {
                abChangeY = Math.floor(abChangeY/row_height) * (changeY > 0 ? 1 : -1);
                self.config.set({'max_rows':initMax + abChangeY}, { validate: true });
            }
        } 
        var cleanup_resize = function(evt) {
            $(window).off("mousemove", table_resize);
        }
        
        $(window).on("mousemove", table_resize);
        $(window).one("mouseup", cleanup_resize);
    },
    
    resizeTableToCtnr: function() {
        var newWidth = this.$el.parent().width() - 1; // -1 for firefox!
        var curWidth = this.$('.tabled').width();
        var delta = newWidth - curWidth;
        var resizableColCount = this.columns.reduce(function(memo, column) {
            return column.get('lock_width') ? memo : ++memo ;
        }, 0);
        var change = delta / resizableColCount;
        this.columns.each(function(col){
            var curWidth = col.get("width");
            col.set({"width": change*1 + curWidth*1}, {validate: true});
        });
    },
    
    updateComparator: function(fn) {
        this.collection.comparator = fn;
        if (typeof fn === "function") this.collection.sort();
    },
    
    restorePreviousState: function() {
        // Check widths
        var widths = this.state('column_widths');
        if (widths !== undefined) {
            _.each(widths, function(val, key, list){
                var col = this.columns.get(key);
                if (col) col.set('width', val);
                else {
                    delete list[key];
                    this.state('column_widths', list);
                }
            }, this);
        }
        
        // Check for column sort order
        var colsorts = this.state('column_sorts');
        if (
            colsorts !== undefined && 
            colsorts.length === this.columns.length &&
            this.columns.every(function(col){ return colsorts.indexOf(col.get('id')) > -1 })
        ) {
            this.columns.col_sorts = colsorts;
            this.columns.sort();
        }

        // Check for column filters
        var colfilters = this.state('column_filters');
        if (
            colfilters !== undefined &&
            colfilters.length === this.columns.length
        ) {
            _.each(colfilters, function(filter) {
                var column = this.columns.get(filter.id);
                if (column) {
                    column.set('filter_value', filter.filter_value);
                }
            }, this);
        }
        
        // Check for max_rows
        var max_rows = this.state('max_rows');
        if (max_rows) {
            this.config.set({'max_rows':max_rows},{validate:true});
        }
    },
    
    state: function(key, value) {
        var storage_key = 'tabled.'+this.config.get("id");
        var store = this.store || localStorage.getItem(storage_key) || {};
        
        if (typeof store !== "object") {
            try {
                store = JSON.parse(store);
            } catch(e) {
                store = {};
            }
        }
        this.store = store;
        
        if (value !== undefined) {
            store[key] = value;
            localStorage.setItem(storage_key, JSON.stringify(store));
            return this;
        } else {
            return store[key];
        }
    },

    getFilteredRows: function() {
        return this.collection.filter(this.config.passesFilters.bind(this.config));
    },

    setLoading: function() {
        this.$('.tbody').html('<div class="tr loading-tr"><span class="tabled-spinner"></span> loading</div>');
    }

});

exports = module.exports = Tabled