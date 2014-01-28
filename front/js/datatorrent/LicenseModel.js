var BaseModel = require('./BaseModel');
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
	}

});
exports = module.exports = LicenseModel;