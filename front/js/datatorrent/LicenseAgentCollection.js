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
var BaseCollection = require('./BaseCollection');
var BaseUtil = require('./BaseUtil');
var LicenseAgentModel = require('./LicenseAgentModel');

/**
 * Collection representing one or more license agents
 */
var LicenseAgentCollection = BaseCollection.extend({

    debugName: 'License Agents',

    model: LicenseAgentModel,

    url: function() {
        return this.resourceURL('LicenseAgent');
    },

    fetchError: BaseUtil.quietFetchError,

    responseTransform: 'licenseAgents'

});
exports = module.exports = LicenseAgentCollection;