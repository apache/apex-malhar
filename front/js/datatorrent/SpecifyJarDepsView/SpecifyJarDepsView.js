var _ = require('underscore');
var Backbone = require('backbone');
var text = require('../text');
var kt = require('knights-templar');
var BaseView = require('../ModalView');

/**
 * Modal view for specifying dependency jars
 */
var SpecifyJarDepsView = BaseView.extend({

	title: text('Specify Jar Dependencies'),

	confirmText: text('save dependencies'),

	body: function() {
		return this.template({

		});
	},

	template: kt.make(__dirname+'/SpecifyJarDepsView.html','_')

});
exports = module.exports = SpecifyJarDepsView;