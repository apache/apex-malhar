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
 * Header View
 * 
 * Contains basic information about the currently loaded recording,
 * or a status of the recording, eg. loading or not found.
*/

var _ = require('underscore');
var kt = require('knights-templar');
var bormat = require('bormat');
var BaseView = require('bassview');
var HeaderView = BaseView.extend({
    
    render: function() {
        var json = this.model.serialize();
        // start time
        if (json.startTime) {
            var st = json.startTime * 1;
            var timeStringFn = +new Date() - st < 86400000 ? 'toLocaleTimeString' : 'toLocaleString';
            json.startTime = new Date(st)[timeStringFn]();
        }
        json['totalTuples'] = bormat.commaGroups(json['totalTuples']);
        var markup = this.template(json);
        this.$el.html(markup);
        return this;
    },
    
    template: kt.make(__dirname+'/HeaderView.html','_')
    
});
exports = module.exports = HeaderView;