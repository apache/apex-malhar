var _ = require('underscore');
var kt = require('knights-templar');
var text = require('../text');
var BaseView = require('../ModalView');

/**
 * Modal that shows console information
 */
var ConsoleInfoModalView = BaseView.extend({

    title: text('UI Console Information'),

    body: function() {
        return this.template({});
    },

    confirmText: text('close'),

    cancelText: false,

    template: kt.make(__dirname+'/ConsoleInfoModalView.html','_')

});
exports = module.exports = ConsoleInfoModalView;