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
 * Bind a backbone model attribute to a text input view
 * 
*/

var _ = require('underscore'), Base = require('./InputView');
var TextInputView = Base.extend({
    
    default_events: ['blur','keyup'],
    
    render: function() {
        this.$el.val(this.model.get(this.attr));
        return this;
    },
    
    updateValue: function(evt) {
        
        if (this._sto) {
            clearTimeout(this._sto);
        }
        
        var update = function(){
            var val = this.$el.val();
            var updates = {};
            updates[this.attr] = val;
            return this.setValue(updates);
        }.bind(this);
        
        if (this.options.delayUpdate) {
            this._sto_ = setTimeout(update, 500);
        } else {
            update();
        }
        
    }
    
});
exports = module.exports = TextInputView;