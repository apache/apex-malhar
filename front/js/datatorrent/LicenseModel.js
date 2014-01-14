var BaseModel = require('./BaseModel');

/**
 * Model representing licenses for datatorrent cluster
 */
var LicenseModel = BaseModel.extend({
	
	debugName: 'license',

	defaults: {
		license: false,
		sublicenses: []
	},

	url: function() {
		return this.resourceURL('License');
	}

});
exports = module.exports = LicenseModel;