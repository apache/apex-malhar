var BaseModel = require('./BaseModel');

var GatewayInfoModel = BaseModel.extend({

	defaults: {
		version: false
	},

	url: function() {
		return this.resourceURL('GatewayInfo');
	}

});

exports = module.exports = GatewayInfoModel;