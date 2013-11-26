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
var BaseView = require('bassview');

var ThCell = BaseView.extend({
    
    className: 'th',
    
    template: _.template('<div class="cell-inner" title="<%= label %>"><span class="th-header"><%= label %></span></div><% if(lock_width !== true) {%><span class="resize"></span><%}%>'),
    
    initialize: function() {
        this.listenTo(this.model, 'change:sort_value', this.render );
        this.listenTo(this.model, 'change:width', function(model, width) {
            this.$el.width(width);
        });
    },
    
    render: function() {
        var json = this.model.serialize();
        var sort_class = json.sort_value ? (json.sort_value == 'd' ? 'desc' : 'asc' ) : '' ;
        this.$el
            .removeClass('asc desc')
            .addClass('col-'+json.id+' '+sort_class)
            .width(json.width)
            .html(this.template(json));
        if (sort_class !== '') {
            this.$('.th-header').prepend('<i class="'+sort_class+'-icon"></i> ');
        }
        return this;
    },
    
    events: {
        'mousedown .resize': 'grabResizer',
        'dblclick .resize': 'fitToContent',
        'mousedown': 'grabColumn'
    },
    
    grabResizer: function(evt) {
        evt.preventDefault();
        evt.stopPropagation();
        var self = this;
        var mouseX = evt.clientX;
        var columnWidth = this.model.get('width');
        // Handler for when mouse is moving
        var col_resize = function(evt) {
            var column = self.model;
            var change = evt.clientX - mouseX;
            var newWidth = columnWidth + change;
            if ( newWidth < column.get('min_column_width')) return;
            column.set({'width': newWidth}, {validate: true});
        };
        var cleanup_resize = function(evt) {
            $(window).off('mousemove', col_resize);
        };
        
        $(window).on('mousemove', col_resize);
        $(window).one('mouseup', cleanup_resize);
    },
    
    fitToContent: function(evt) {
        evt.preventDefault();
        evt.stopPropagation();
        var new_width = 0;
        var min_width = this.model.get('min_column_width');
        var id = this.model.get('id');
        var $ctx = this.$el.parents('.tabled').find('.tbody');
        $('.td.col-'+id+' .cell-inner', $ctx).each(function(i, el){
            new_width = Math.max(new_width,$(this).outerWidth(true), min_width);
        });
        this.model.set({'width':new_width},{validate: true});
    },
    
    changeColumnSort: function(evt) {
        var model = this.model;
        
        if (typeof model.get('sort') !== 'function' || model.get('moving') === true) return;
        var cur_sort = model.get('sort_value');
        if (!evt.shiftKey) {
            // disable all sorts
            model.collection.each(function(col){
                if (col !== model) col.set({'sort_value': ''});
            });
        }
        switch(cur_sort) {
            case 'a':
                model.set('sort_value', 'd');
                break;
            case 'd':
                model.set('sort_value', 'a');
                break;
            default:
                model.set('sort_value', 'a');
                break;
        }
    },
    
    grabColumn: function(evt) {
        if ( evt.button === 2 ) return;
        evt.preventDefault();

        var self = this;
        var mouseX = evt.clientX;
        var offsetX = evt.offsetX;
        var thresholds = [];
        var currentIdx = this.model.sortIndex();
        var $tr = this.$el.parent();
        $tr.find('.th').each(function(i,el){
            var $this = $(this);
            var offset = $this.offset().left;
            var half = $this.width() / 2;
            if (i != currentIdx) thresholds.push(offset+half);
        });
        var prevent_mouseup = false;
        
        var getNewIndex = function(pos) {
            var newIdx = 0;
            for (var i=0; i < thresholds.length; i++) {
                var val = thresholds[i];
                if (pos > val) newIdx++;
                else return newIdx;
            }
            return newIdx;
        };
        
        var drawHelper = function(newIdx) {
            $tr.find('.colsort-helper').remove();
            if (newIdx == currentIdx) return;
            var method = newIdx < currentIdx ? 'before' : 'after';
            $tr.find('.th:eq('+newIdx+')')[method]('<div class="colsort-helper"></div>');
        };
        
        var move_column = function(evt) {
            var curMouse = evt.clientX;
            var change = curMouse - mouseX;
            self.$el.css({'left':change, 'opacity':0.5, 'zIndex': 10});
            
            var newIdx = getNewIndex(curMouse, thresholds);
            drawHelper(newIdx);
            self.model.set('moving', true);
        };
        
        var cleanup_move = function(evt) {
            self.$el.css({'left': 0, 'opacity':1});
            $tr.find('.colsort-helper').remove();
            var curMouse = evt.clientX;
            var change = curMouse - mouseX;
            self.model.sortIndex(getNewIndex(curMouse, thresholds));
            $(window).off('mousemove', move_column);
            self.model.set('moving', false);
            
            // In case movement was so small that 
            // it probably was intended to change 
            // the column sort order
            if (Math.abs(change) < 8) {
                self.changeColumnSort.call(self,evt);
            }
        };
        
        $(window).on('mousemove', move_column);
        $(window).one('mouseup', cleanup_move);
    }
});

var ThRow = BaseView.extend({
    
    render: function() {
        // clear it
        this.$el.empty();
        
        // render each th cell
        this.collection.each(function(column){
            var view = new ThCell({ model: column });
            this.$el.append( view.render().el );
            view.listenTo(this, 'clean_up', view.remove);
        }, this);
        return this;
    }
    
});

var FilterCell = BaseView.extend({
    
    initialize: function() {
        this.listenTo(this.model, 'change:width', function(column, width){
            this.$el.width(width);
        });
    },
    
    template: _.template([
        '<div class="cell-inner">',
            '<input ',
                'class="filter <% if (filter_value) { print("active") } %>" ',
                'value="<%= filter_value %>" ',
                'type="search" placeholder="filter" ',
            '/>',
        '</div>'].join('')),
    
    render: function() {
        var fn = this.model.get('filter');
        var markup = typeof fn === 'function' ? this.template(this.model.toJSON()) : '' ;
        this.$el.addClass('td col-'+this.model.get('id')).width(this.model.get('width'));
        this.$el.html(markup);
        return this;
    },
    
    events: {
        'click .filter': 'updateFilter',
        'keyup .filter': 'updateFilterDelayed'
    },
    
    updateFilter: function(evt) {
        var value = $.trim(this.$('.filter').val());
        var method = value ? 'addClass' : 'removeClass';
        this.model.set( 'filter_value', value );
        this.$('input.filter')[method]('active');
    },
    
    updateFilterDelayed: function(evt) {
        if (this.updateFilterTimeout) clearTimeout(this.updateFilterTimeout);
        this.updateFilterTimeout = setTimeout(this.updateFilter.bind(this, evt), 200);
    }
    
});

var FilterRow = BaseView.extend({
    
    render: function() {
        // clear it
        this.$el.empty();
        this.trigger('clean_up');

        // render each th cell
        this.collection.each(function(column){
            var view = new FilterCell({ model: column });
            this.$el.append( view.render().el );
            view.listenTo(this, 'clean_up', view.remove);
        }, this);
        return this;
    }
    
});

var Thead = BaseView.extend({
    
    initialize: function(options) {
        // Set config
        this.config = options.config;
        
        // Setup subviews
        this.subview('th_row', new ThRow({ collection: this.collection }));
        
        if (this.needsFilterRow()) {
            this.subview('filter_row', new FilterRow({ collection: this.collection }));
        }
        
        // Listen for when offset is not zero
        this.listenTo(this.config, 'change:offset', function(model, offset){
            var toggleClass = offset === 0 ? 'removeClass' : 'addClass' ;
            this.$el[toggleClass]('overhang');
        });
    },
    
    template: '<div class="tr th-row"></div><div class="tr filter-row"></div>',
    
    render: function() {
        this.$el.html(this.template);
        this.assign({ '.th-row' : 'th_row' });
        if (this.subview('filter_row')) {
            this.assign({ '.filter-row' : 'filter_row' });
        }
        else this.$('.filter-row').remove();
        return this;
    },
    
    needsFilterRow: function() {
        return this.collection.some(function(column){
            return (typeof column.get('filter') !== 'undefined');
        });
    }
    
});

exports = module.exports = Thead;