var BaseView = require('bassview');
var StepView = BaseView.extend({

    initialize: function(options) {
        options = options || {};
        this.template = options.template;
    },

    render: function() {
        var html = this.template({});
        this.$el.html(html);
        if (this.assignments) {
            this.assign(this.assignments);
        }
        return this;
    }

});
exports = module.exports = StepView;