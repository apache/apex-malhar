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
 * Bind a backbone model attribute to a select input view
 * 
*/

var _ = require('underscore'), Base = require('./InputView');
var SelectInputView = Base.extend({
    
    default_events: ['change'],
    
    initialize: function(options) {
        Base.prototype.initialize.call(this,options);
        
        if (this.collection) {
            
            if (options.colAttr === undefined) {
                throw new Error("'colAttr' is a required option for a select bbinding!");
            }
            
        }
    },
    
    render: function() {
        if (this.collection) {
            var html = '';
            var valAttr = this.colAttr
            this.collection.each(function(item) {
                var value = item.get(valAttr);
                html += '<option value="' + value + '">' + value + '</option>';
            });
            this.$el.html(html);
        }
        this.$el.val(this.model.get(this.attr));
        return this;
    },
    
    updateValue: function(evt) {
        var val = this.$el.val();
        var updates = {};
        updates[this.attr] = val;
        return this.setValue(updates);
    }
    
});
exports = module.exports = SelectInputView;