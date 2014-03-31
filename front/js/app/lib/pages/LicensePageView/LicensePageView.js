var _ = require('underscore');
var kt = require('knights-templar');
var BaseView = require('bassview');

var LicenseFileCollection = require('../../widgets/ConfigWelcomeWidget/LicenseFileCollection');
var UploadLicenseView = require('../../widgets/ConfigWelcomeWidget/UploadLicenseView');

var LicensePageView = BaseView.extend({

    initialize: function(options) {

        // Set a collection for the jar(s) to be uploaded
        this.filesToUpload = new LicenseFileCollection([], {
        });

        this.subview('file-upload', new UploadLicenseView({
            collection: this.filesToUpload
        }));

        this.listenTo(this.filesToUpload, 'upload_success', function() {
            console.log('success');
        });

        this.listenTo(this.filesToUpload, 'upload_error', function (jqXHR) {
            console.log('failure');
        });

        this.license = options.app.license;
        this.listenTo(this.license.get('agent'), 'sync', this.render);
    },

    render: function() {
        var json = this.license.toJSON()
        var html = this.template(json);
        this.$el.html(html);
        this.assign('.file-upload-target', 'file-upload');
        return this;
    },

    template: kt.make(__dirname+'/LicensePageView.html')

});

exports = module.exports = LicensePageView;