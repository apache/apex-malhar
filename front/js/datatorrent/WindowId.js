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
var BigInteger = require('jsbn');

function WindowId(value) {

    this.set(value);

}

WindowId.prototype = {
    
    toString: function() {
        // return formatter.windowFormatter(this);
        if (this.offset === '-') {
            return '-';
        } else if (this.offset === false) {
            return '<span title="invalid window ID: ' + this.value + '">Invalid</span>';
        }
        return '<span class="window-id-display" title="timestamp: ' + this.timestamp.toLocaleString() + '">' + this.offset + '</span>';
    },
    
    set: function(value) {
        value = this.value = '' + value;

        // Check for initial values of 0 and -1
        if (value === '-1' || value === '0' || value === 'undefined') {
            this.timestamp = '-';
            this.offset = '-';
            return;
        }

        if (/[^0-9]/.test(value)) {
            LOG(3, 'WindowId first param not in expected format: ', value);
            this.timestamp = '-';
            this.offset = false;
            return;
        }
	
        var full64 = new BigInteger(this.value);
    
        this.timestamp = new Date((full64.shiftRight(32).toString() + '000') * 1);
    
        this.offset = full64.and(new BigInteger('0x00000000ffffffff',16)).toString() * 1;
    }
    
};

exports = module.exports = WindowId;