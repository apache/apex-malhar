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
var kt = require('knights-templar');
var BaseView = require('bassview');
var BigInteger = require('jsbn');
var TupleView = BaseView.extend({
    
    initialize: function(options) {
        this.recording = options.recording;
        this.listenTo(this.model, 'change', this.render);
    },
    
    render: function() {
        
        var json = this.model.serialize(this.recording);
        var markup = this.template(json);
        this.$el.html(markup);
        
        return this;
    },
    
    events: {
        'click a.tuple': 'onTupleClick' 
    },
    
    onTupleClick: function(evt) {
        // BigInteger
        evt.preventDefault();
        var tupleCol = this.recording.tuples;
        var selected = tupleCol.where({selected:true});
        if (!evt.shiftKey) {
            tupleCol.deselectAll();
            this.model.set({'selected': true, 'anchored': true});
        } else if (this.model.get('selected') && selected.length == 1) {
            this.model.set('selected', false);
        } else {
            var anchor = tupleCol.where({anchored:true})[0];
            tupleCol.deselectAll();
            var anchor_index = new BigInteger(anchor.get('idx'));
            var this_index = new BigInteger(this.model.get('idx'));
            var min = anchor_index.min(this_index);
            var max = anchor_index.max(this_index).add(BigInteger.ONE);
            var i = min;
            while (/^\-/.test(i.compareTo(max).toString())) {
                var otherTuple = tupleCol.get(i.toString());
                if (otherTuple) otherTuple.set('selected', true);
                i = i.add(BigInteger.ONE)
            }
            
            anchor.set('anchored',true);
        }
        
        // update the console
        this.recording.trigger('update_console');
    },
    
    template: kt.make(__dirname+'/TupleView.html','_')
    
});

exports = module.exports = TupleView