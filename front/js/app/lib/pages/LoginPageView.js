var _ = require('underscore');
var kt = require('knights-templar');
var BasePage = require('bassview');
var LoginPageView = BasePage.extend({

    initialize: function(options) {
        this.model = options.app.user;
    },

    render: function() {
        var json = this.model.toJSON();
        var html = this.template(json);
        this.$el.html(html);
        return this;
    },

    events: {
        'submit #login-form': 'onSubmit'
    },

    onSubmit: function() {
        console.log('submitting form...');
    },

    template: kt.make(__dirname+'/LoginPageView.html')

});
exports = module.exports = LoginPageView;