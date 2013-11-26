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
 * ActionWidget
 * 
 * Description of widget.
 *
*/

var _ = require('underscore');
var kt = require('knights-templar');
var BaseView = require('../../WidgetView');

// class definition
var ActionWidget = BaseView.extend({
    
    initialize: function(options) {
        
        BaseView.prototype.initialize.call(this, options);
        
        // listeners, subviews, etc.
        
    },
    
    html: function() {
        
        var html = '';
        
        // generate markup here
        
        return html;
    },
    
    assignments: {
        
        // assign subviews here
        
    },
    
    template: kt.make(__dirname+'/ActionWidget.html','_')
    
});

exports = module.exports = ActionWidget;