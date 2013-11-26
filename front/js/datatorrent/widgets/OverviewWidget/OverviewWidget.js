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
 * OverviewWidget
 * 
 * Description of widget.
 *
*/

var _ = require('underscore');
var kt = require('knights-templar');
var BaseView = require('../../WidgetView');

// class definition
var OverviewWidget = BaseView.extend({
    
    initialize: function(options){
        // Call super init
        BaseView.prototype.initialize.call(this, options);
        
        // Listen for changes
        this.listenTo(this.model, "change", this.renderContent);
    },
    
    contentClass: 'page-overview',
    
});

exports = module.exports = OverviewWidget;