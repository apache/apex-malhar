var BaseModel = require('./BaseModel');
var LicenseAgents = require('./LicenseAgentCollection');

/**
 * Model representing licenses for datatorrent cluster
 */
var LicenseModel = BaseModel.extend({
	
	debugName: 'license',

	defaults: {
		licenseAgents: new LicenseAgents([])
	},

    toJSON: function() {
        var json = BaseModel.prototype.toJSON.call(this);
        json.licenseAgents = json.licenseAgents.toJSON();
        return json;
    },

	url: function() {
		return this.resourceURL('License');
	}

});
exports = module.exports = LicenseModel;