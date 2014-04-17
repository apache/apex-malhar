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
var BaseModel = require('./BaseModel');
var BaseUtil = require('./BaseUtil');
var LicenseAgent = require('./LicenseAgentModel');

/**
 * Model representing licenses for datatorrent cluster
 */
var LicenseModel = BaseModel.extend({
	
	debugName: 'license',

	defaults: {
		agent: new LicenseAgent({}),
        sections: []
	},

    toJSON: function() {
        var json = BaseModel.prototype.toJSON.call(this);
        json.agent = json.agent.toJSON();
        return json;
    },

	url: function() {
		return this.resourceURL('License');
	},

    fetch: function(options) {
        this.once('sync', function(model) {
            var agent = this.get('agent');
            agent.set('id', model.get('id'));
            agent.fetch({
                agentMaxTries: typeof options === 'object' ? options.agentMaxTries : 0
            });
        });
        return BaseModel.prototype.fetch.apply(this, arguments);
    },

    isDefault: function () {
        return this.get('id') && (this.get('id').indexOf('default') === 0);
    },

    fetchError: BaseUtil.quietFetchError

});
exports = module.exports = LicenseModel;