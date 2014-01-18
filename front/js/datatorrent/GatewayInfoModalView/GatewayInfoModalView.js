var _ = require('underscore');
var kt = require('knights-templar');
var text = require('../text');
var GatewayInfoModel = require('../GatewayInfoModel');
var BaseView = require('../ModalView');

/**
 * Modal that shows license information
 */
var GatewayInfoModalView = BaseView.extend({

	title: text('Gateway Information'),

	initialize: function() {
		this.model = new GatewayInfoModel({});
		this.model.fetch();
		this.listenTo(this.model, 'sync', this.renderBody);
	},

	body: function() {
		var json = this.model.toJSON();
		var html = this.template(json);
		return html;
	},

	confirmText: text('close'),

	cancelText: false,

	template: kt.make(__dirname+'/GatewayInfoModalView.html','_')

});
exports = module.exports = GatewayInfoModalView;