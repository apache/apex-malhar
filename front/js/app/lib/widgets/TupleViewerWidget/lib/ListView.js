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
 * Tuple List view
 * 
 * This view shows the actual tuples in the current viewport.
*/
var _ = require('underscore');
var kt = require('knights-templar');
var Tuple = DT.lib.TupleModel;
var TupleView = require('./TupleView');
var BaseView = require('bassview');
var BigInteger = require('jsbn');
var ListView = BaseView.extend({
    
    initialize: function() {
        this.listenTo(this.model, "change:offset", this.render);
        this.listenTo(this.model.tuples, "reset", this.render);
    },
    
    render: function() {
        
        // Remove all views
        this.trigger('remove_tupleviews');
        
        // Fill initial markup
        var json = this.model.serialize();
        var markup = this.template(json);
        this.$el.html(markup);
        
        // check if any recording there to render
        if (!json.appId || !json.operatorId || !json.startTime || json.currentTotal == '0') {
            return this;
        }
        
        
        this.tupleViews = [];
        var startOffset = new BigInteger(json.offset+'');
        var endOffset = startOffset.add(new BigInteger(json.limit+'')).subtract(BigInteger.ONE);
        var curTupleCol = this.model.tuples;
        var i = new BigInteger(json.offset+'');
        var $inner = this.$('.inner');
        
        while ( i.compareTo(endOffset) != 1 ) {
            
            var tuple;
            var index = i.toString();
            
            // first check for model in curTupleCol
            tuple = curTupleCol.get(index);
            if (tuple === undefined) {
                
                // create new model for collection
                tuple = new Tuple({
                    idx: index
                });
                curTupleCol.add(tuple);
                
            }
            // create new tupleview
            var view = new TupleView({
                model: tuple,
                recording: this.model
            }).render();
            
            // add to the inner element
            $inner.append(view.el);
            
            // listen to removal
            view.listenToOnce(this, 'remove_tupleviews', view.remove);
            
            // increment iterator
            i = i.add(BigInteger.ONE);
            
        }
        
        return this;
    },
    
    events: {
        "mousewheel": "onMouseWheel",
        "wheel":"onMouseWheel"
    },
    
    onMouseWheel: function(evt) {
        evt.preventDefault();
        evt.originalEvent.preventDefault();
        var json = this.model.serialize();
        
        // normalize webkit/firefox scroll values
        var deltaY = -evt.originalEvent.wheelDeltaY || evt.originalEvent.deltaY * 100;
        
        // move by a decent amount
        var movement = Math.round(deltaY / 100);
        if (isNaN(movement)) return;
        var origOffset = new BigInteger(json.offset+'');
        var offset = origOffset.add(new BigInteger(movement+''));
        var upper = new BigInteger(json.currentTotal).subtract(new BigInteger(json.limit+''));
        offset = offset.min(upper).max(BigInteger.ZERO);
        this.model.set({"offset": offset.toString()}, {validate: true});
    },
    
    template: kt.make(__dirname+'/ListView.html','_')
});

exports = module.exports = ListView