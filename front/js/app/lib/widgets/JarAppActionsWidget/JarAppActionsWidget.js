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
 * JarAppActionsWidget
 * 
 * Description of widget.
 *
*/

var _ = require('underscore');
var kt = require('knights-templar');
var BaseView = DT.lib.WidgetView;

// class definition
var JarAppActionsWidget = BaseView.extend({

	className: 'visible-overflow',
    
    events: {
        'click .launchApp':'launchApp',
        'click .launchAppProps': 'launchAppProps'
    },
    
    launchApp: function(evt) {

        // disable the button
        $(evt.target).prop('disabled', true);

        // launch the app without 
        this.model.launch();
    },

    launchAppProps: function(evt) {
    	
        // Prevent link from going to #
        evt.preventDefault();

        // Launch app and specify properties
        this.model.launch(true);
    },

    template: kt.make(__dirname + '/JarAppActionsWidget.html','_'),
    
});

exports = module.exports = JarAppActionsWidget;