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
var _                  = require('underscore'),
    Widget             = require('./<%= name %>'),
    WidgetDefModel     = require('../../WidgetDefModel'),
    DashModel          = require('../../DashModel'),
    WidgetClasses      = require('../../WidgetClassCollection');


describe('<%= name %>.js', function() {
   
    var sandbox, dashDef, widgetDef, options, $box;
    
    beforeEach(function() {
        // Use this for creating spies, mocks, stubs
        sandbox = sinon.sandbox.create();
        
        // Dashboard definition (mock)
        dashDef = new DashModel({ 
            widgets: new WidgetClasses([
                {
                    name: 'Widget',
                    view: Widget,
                    limit: 1
                }
            ])
        });
        
        // Widget definition (mock)
        widgetDef = new WidgetDefModel({
            'widget': 'Widget',
            'id': 'testingWidget'
        });
        
        // Options to use for constructing widget instance
        options = {
            dashboard: dashDef,
            widget: widgetDef
        };
        
        // Create an element for use in tests
        $box = $('<div id="sandbox" style="display:none;"></div>').appendTo('body');
        
    });
    
    afterEach(function() {
        sandbox.restore();
        $box.remove()
        dashDef = widgetDef = options = $box = null;
    });
    
    
    
});