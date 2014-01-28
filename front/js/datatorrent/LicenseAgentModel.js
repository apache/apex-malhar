var BaseModel = require('./BaseModel');

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
        json.percentUsedLicenseMB = ((json.usedLicensedMB / json.totalLicensedMB) * 100).toFixed(1);
        return json;
    },

    urlRoot: function() {
        return this.resourceURL('LicenseAgent')
    }

});
exports = module.exports = LicenseAgentModel;