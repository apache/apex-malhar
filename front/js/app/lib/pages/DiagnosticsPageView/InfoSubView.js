var _ = require('underscore');
var kt = require('knights-templar');
var BaseView = require('bassview');
var InfoSubView = BaseView.extend({

    initialize: function(options) {
        this.gatewayConnectAddress = new DT.lib.ConfigPropertyModel({ name: 'dt.attr.GATEWAY_CONNECT_ADDRESS' });
        this.dfsRootDirectory = new DT.lib.ConfigPropertyModel({ name: 'dt.dfsRootDirectory' });
        this.about = new DT.lib.GatewayInfoModel();

        this.allPromise = $.when(
            this.gatewayConnectAddress.fetch(),
            this.dfsRootDirectory.fetch(),
            this.about.fetch()
        );

        this.allPromise.always(this.render.bind(this));
    },

    render: function() {
        var json = {
            fetchState: this.allPromise.state(),

            gatewayConnectAddress: this.gatewayConnectAddress.toJSON(),
            dfsRootDirectory: this.dfsRootDirectory.toJSON(),
            about: this.about.toJSON()
        };
        var html = this.template(json);
        this.$el.html(html);
        return this;
    },

    template: kt.make(__dirname+'/InfoSubView.html')

});
exports = module.exports = InfoSubView;