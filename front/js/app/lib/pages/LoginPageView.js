var _ = require('underscore');
var kt = require('knights-templar');
var BasePage = require('bassview');
var Bbind = DT.lib.Bbindings;
var LoginPageView = BasePage.extend({

    initialize: function(options) {
        this.nav = options.app.nav;
        this.model = options.app.user;
        this.subview('userName', new Bbind.text({
            model: this.model,
            attr: 'userName',
            clearErrorOnFocus: true,
            listenToModel: false,
            setAnyway: true,
            classElement: function($el) {
                return $el.parent().parent();
            },
            errorEl: '.help-block',
            errorClass: 'error',
            updateEvents: ['blur', 'update']
        }));
        this.subview('password', new Bbind.text({
            model: this.model,
            attr: 'password',
            clearErrorOnFocus: true,
            listenToModel: false,
            setAnyway: true,
            classElement: function($el) {
                return $el.parent().parent();
            },
            errorEl: '.help-block',
            errorClass: 'error',
            updateEvents: ['blur', 'update']
        }));
    },

    render: function() {
        var json = {
            user: this.model.toJSON(),
            error: this.error
        };
        var html = this.template(json);
        this.$el.html(html);
        this.assign({
            '#userName': 'userName',
            '#password': 'password'
        })
        return this;
    },

    events: {
        'submit #login-form': 'onSubmit'
    },

    onSubmit: function(e) {
        e.preventDefault();
        if (this.model.isValid()) {
            var self = this;
            var promise = this.model.login();
            promise
                .fail(function(xhr, errorThrown, responseText) {
                    self.error = 'An error occurred while trying to log in. Ensure that you entered the correct credentials. (' + xhr.status + ' ' + responseText + ')';
                    self.render();
                })
                .done(function() {
                    self.nav.redirectAuthenticatedUser();
                });
        } else {
            this.subview('userName').$el.trigger('update');
            this.subview('password').$el.trigger('update');
        }
    },

    template: kt.make(__dirname+'/LoginPageView.html')

});
exports = module.exports = LoginPageView;