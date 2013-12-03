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
 * Text input for integers only
 * 
*/
var Base = require('./TextInputView');
var IntTextInputView = Base.extend({
    
    _acceptedCodes: [8,9,16,17,18,37,38,39,40,48,49,50,51,52,53,54,55,56,57,91],
    
    events: function() {
        var eventHash = Base.prototype.events.call(this);
        eventHash['keydown'] = 'checkForDownUp';
        return eventHash;
    },
    
    checkForDownUp: function(evt) {
        
        var code = evt.keyCode;
        var val = this.$el.val();
        var change = 0;
        
        // Prevent letters and other characters
        if (code && this._acceptedCodes.indexOf(code) === -1) {
            evt.preventDefault();
            return false;
        }
        
        // Up and down should change the value those directions
        if (code === 38) { // up
            change = evt.shiftKey ? 10 : 1 ;
        } else if (code === 40) { // down
            change = evt.shiftKey ? -10 : -1 ;
        } else {
            return;
        }
        
        this.$el.val(val*1 + change*1);
    }
    
});
exports = module.exports = IntTextInputView;