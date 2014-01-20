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
var _ = require('underscore');
var Backbone = require('backbone');
var Notifier = DT.lib.Notifier;
var BasePageView = DT.lib.BasePageView;

var ErrorPageView = BasePageView.extend({

    pageName: 'ErrorPageView',

    useDashMgr: false,

    initialize: function(attributes, options) {
        BasePageView.prototype.initialize.call(this, attributes, options);
        console.log('__ErrorPageView');
    },

    render: function() {
            Notifier.error({
                'title': 'Page Not Found',
                'text': 'Requested page is not found.',
                'delay': 5000
            });
    }

});

exports = module.exports = ErrorPageView