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
 * Console View
 * 
 * Main control area for the tupleviewer widget
*/

var _ = require('underscore');
var BigInteger = require('jsbn');
var kt = require('knights-templar');
var PortsView = require('./PortsView');
var BaseView = require('bassview');
var ConsoleView = BaseView.extend({
    
    initialize: function() {
        this.subview('ports', new PortsView({ collection: this.model.ports }));
        // this.listenTo(this.model, 'change:currentWindowId change:currentTupleIndex', this.render);
        this.listenTo(this.model, 'update_console', this.render);
        this.listenTo(this.model.tuples, 'update', this.render);
        // this.listenTo(this.model, 'change:currentWindowId change:currentTupleIndex', function() {
        //     console.log('1');
        //     this.render();
        // });
        // this.listenTo(this.model.tuples, 'update', function() {
        //     console.log('2');
        //     this.render();
        // });
    },
    
    className: 'console',
    
    render: function() {
        var json = this.model.serialize();
        var markup = this.template(json);
        this.$el.html(markup);
        this.assign('.ports-frm','ports');
        return this;
    },
    
    events: {
        
        'click .item.window .controls .prev': 'previousWindow',
        'click .item.window .controls .next': 'nextWindow',
        'click .item.tuple .controls .prev': 'previousTuple',
        'click .item.tuple .controls .next': 'nextTuple',
        'dblclick .currentWindowId': 'directEnterWindow',
        'dblclick .currentTupleIndex': 'directEnterIndex' 
        
    },
    
    previousWindow: function(evt) {
        this.model.stepToWindow(-1);
    },
    nextWindow: function(evt) {
        this.model.stepToWindow(1);
    },
    previousTuple: function(evt) {
        this.model.shiftSelected(-1);
    },
    nextTuple: function(evt) {
        this.model.shiftSelected(1);
    },
    
    directEnterIndex: function(evt) {
        evt.preventDefault();
        evt.originalEvent.preventDefault();
        var $el = this.$('.currentTupleIndex'),
            previous = $el.text(),
            $input = $('<input type="text" class="newTupleIndex" value="' + previous + '" style="width:90%; font:inherit" />');
        
        $el.html($input)
        $input
            .one('blur', _.bind(function(evt){
                var value = $input.val() * 1;
                var biValue = new BigInteger(value + '');
                var total = this.model.ports.reduce(function(memo, port) {
                    if (port.get('selected')) {
                        memo = memo.add(new BigInteger(port.get('tupleCount') + ''));
                    }
                    return memo;
                }, BigInteger.ZERO);
                total = total.subtract(new BigInteger(this.model.get('limit')+''));
                
                if (total.compareTo(biValue) > 0) {
                    console.log('total larger than inputted value');
                    value = biValue.toString();
                } else {
                    value = total.toString();
                }
                
                var isValid = this.model.set({'offset':value}, {validate:true});
                
                $input.off();
                
                tuple = this.model.tuples.get(value);
                this.model.tuples.deselectAll(true);
                tuple.set({
                    'selected': true,
                    'anchored': true
                });
                this.model.trigger('update_console');
                $el.html(value);
                
            }, this))
            .on('keydown', function(evt) {
                if (evt.which === 13) {
                    $input.trigger('blur');
                }
            })
            .focus();
        
    },
    
    directEnterWindow: function() {
        
    },
    
    template: kt.make(__dirname+'/ConsoleView.html','_')
});

exports = module.exports = ConsoleView