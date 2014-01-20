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
var Backbone = require('backbone');
var WidgetDefModel = Backbone.Model.extend({
    
    defaults: {
        'widget':'',
        'id':'',
        'width':100,
        'height': 'auto'
    },
    
    validate: function(attrs) {
        if (attrs.width < 20 || attrs.width > 100) {
            return 'Widget width must be between 20 and 100 percent';
        }
        if (attrs.height !== 'auto' && typeof attrs.height !== 'number') {
            return 'Widget height must be a number or "auto"';
        } else if (attrs.height < 0) {
            return 'Widget height must be a positive number';
        }
    }
    
});
exports = module.exports = WidgetDefModel;