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
 * NavModel private methods
 *
*/

var _ = require('underscore');

exports = module.exports = {
    
    
    /** Extracts the "routes" key from page module objects and creates a hash consumable by Backbone.Router */
    extractRoutesFrom: function(pages) {
        
        var res = {};
        
        _.each(pages, function(page, index, list) {
            
            _.each(page.routes, function(route, i) {

                res[route] = page.name;

            });
            
        });
        
        return res;
    }
    
}