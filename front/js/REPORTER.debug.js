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
(function(){
    
    var COLORS = {
        // temp log
        0: 'color: #000; font-weight: bold;',
        // outgoing
        1: 'color: #1e82b0; font-weight: normal;',
        // incoming
        2: 'color: #39b050; font-weight: normal;',
        // warning
        3: 'color: #c97406; font-weight: normal;',
        // error
        4: 'color: #c9250a; font-weight: normal;',
        // major
        5: 'color: #c9250a; font-weight: bold;',
        // websocket message
        6: 'color: #17a172; font-weight: bold;'
    };
    
    window.LOG = function(code, msg, more) {
        
        if (!window.console) return;
        
        var args = [ '%c' + msg , COLORS[code] ];
        
        if (more) args = args.concat(more);
        
        console.log.apply(window.console, args);
        
    }
    
}());