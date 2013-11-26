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
var BaseView = require('bassview');
var BigInteger = require('jsbn');
var DataView = BaseView.extend({
    
    initialize: function() {
        this.listenTo(this.model.tuples, "change:selected", this.render, this );
        this.listenTo(this.model.tuples, "change:portId", this.render, this);
    },
    
    render: function() {
        
        this.$el.empty();
        
        var data = this.model.tuples.where({selected: true});
        data.sort(function(a,b){
            var comp = new BigInteger(a.get("idx")).compareTo(new BigInteger(b.get("idx"))).toString();
            if (comp == '0') return 0;
            if (/^\-/.test(comp)) return -1;
            return 1;
        });
        
        var html = "";
        var ports = this.model.ports;
        _.each(data, function(tuple) {
            var port = ports.where({id: tuple.get("portId")})[0];
            var type = port ? port.get("type") : "loading";
            var content = JSON.stringify(tuple.get("data"), null, 4);
            content = _.escape(content);
            html += '<pre class="'+type+'">'+tuple.get("idx")+": "+content+'</pre>';
        }, this);
        
        this.$el.html(html);
        
        return this;
    }
    
});

exports = module.exports = DataView