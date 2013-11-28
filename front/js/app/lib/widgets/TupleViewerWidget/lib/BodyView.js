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
 * Body View for the tupleviewer
 * 
 * Contains everything about the recording. If no recording present,
 * this will render as a single message saying that no recording was found.
 * This is the main reason for the existence of this subview.
*/
var _ = require('underscore');
var kt = require('knights-templar');
var BaseView = require('bassview');
var ConsoleView = require('./ConsoleView');
var ScrollerView = require('./ScrollerView');
var ListView = require('./ListView');
var DataView = require('./DataView');
var BodyView = BaseView.extend({
    
    initialize: function() {
        this.subview('console', new ConsoleView({
            model: this.model
        }));
        this.subview('scroller', new ScrollerView({
            model: this.model
        }));
        this.subview('list', new ListView({
            model: this.model
        }));
        this.subview('data', new DataView({
            model: this.model
        }));
    },
    
    render: function() {
        var json = this.model.serialize();
        
        // check if ok to render everything
        if (!json.startTime) {
            this.$el.empty();
            return this;
        }
        
        var markup = this.template(json);
        this.$el.html(markup);
        this.assign({
            '.console'  : 'console',
            '.scroller' : 'scroller',
            '.list'     : 'list',
            '.dataview' : 'data'
        });
        
        return this;
    },
    
    template: kt.make(__dirname+'/BodyView.html','_')
    
});
exports = module.exports = BodyView;