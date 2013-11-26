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
var TupleModel = Backbone.Model.extend({
    
    defaults: {
        'data': '',
        'idx': '',
        'portId': '',
        'windowId': ''
    },
    
    idAttribute: 'idx',
    
    serialize: function(recording) {
        if (recording === undefined || recording.serialize === undefined) {
            throw new Error('TupleModel.serialize must be provided with a recording object');
        }
        var json = this.toJSON();
        var rjson = recording.serialize();
        var port = json.portId ? recording.ports.where({id: json.portId})[0] : false;
        if (port) {
            json.type = port.get('type');
            json.port = port.get('name');
        }
        else {
            json.type = json.port = 'loading';
        }
        json.height = rjson.innerTupleHeight;
        json.padding = rjson.tuplePaddingTB;
        json.marginBottom = rjson.tupleMarginBtm;
        json.selected = json.selected ? 'selected' : '';
        
        return json;
    }
    
});
exports = module.exports = TupleModel;