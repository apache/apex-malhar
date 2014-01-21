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

var text = require('./text');
var Notifier = require('./Notifier');
var BaseModel = require('./AbstractJarFileModel');
var SpecifyJarDepsView = require('./SpecifyJarDepsView');
var DepJarFileCollection = require('./DepJarFileCollection');

/**
 * Jar File Model
 * 
 * Models a jar file that should contain application plans.
*/
var AppJarFileModel = BaseModel.extend({
    
    specifyDependencies: function() {

    	if (! (this.depJars instanceof DepJarFileCollection) ) {
    		this.depJars = new DepJarFileCollection([]);
    	}
        var modal = new SpecifyJarDepsView({
        	collection: this.depJars,
            model: this
        });

        $('body').append(modal.render().el);

        modal.launch();

    },

    submitDependencies: function() {
    	$.ajax({
    		url: this.resourceAction('specifyDepJars', {
    			fileName: this.get('name')
    		}),
    		method: 'PUT',
    		data: JSON.stringify({
    			dependencyJars: this.depJars.pluck('name')
    		}),
            contentType: 'application/json; charset=utf-8',
            success: function(res) {
            	Notifier.success({
            		title: text('specify_deps_success_title'),
            		text: text('specify_deps_success_text')
            	});
            },
            error: function() {
            	Notifier.error({
            		title: text('specify_deps_error_title'),
            		text: text('specify_deps_error_text')
            	});
            }
    	});
    }
    
});

exports = module.exports = AppJarFileModel;