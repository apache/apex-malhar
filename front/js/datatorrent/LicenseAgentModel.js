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
var LicenseAgentModel = BaseModel.extend({

    debugName: 'License Agent',

    defaults: {
        id: '',
        appId: '',
        startedTime: '',
        user: '',
        remainingLicensedMB: '',
        totalLicensedMB: ''
    },

    toJSON: function() {
        var json = BaseModel.prototype.toJSON.call(this);
        json.usedLicensedMB = json.totalLicensedMB * 1 - json.remainingLicensedMB * 1;
        json.percentUsedLicenseMB = json.remainingLicensedMB ? ((json.usedLicensedMB / json.totalLicensedMB) * 100).toFixed(1) : '';
        return json;
    },

    urlRoot: function() {
        return this.resourceURL('LicenseAgent')
    },

    fetchError: function (object, response, options) {
        this.set('fetchFailed', true);
        BaseUtil.quietFetchError(object, response, options);
    }

});
exports = module.exports = LicenseAgentModel;