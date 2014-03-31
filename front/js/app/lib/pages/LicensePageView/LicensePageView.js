var _ = require('underscore');
var Notifier = DT.lib.Notifier;
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
            Notifier.success({
                title: 'License File Successfully Uploaded',
                text: 'The information on the license page should updated. If it does not, try refreshing the page'
            });
        });

        this.listenTo(this.filesToUpload, 'upload_error', function (jqXHR) {
            Notifier.error({
                title: 'Error Uploading License',
                text: 'Something went wrong while trying to upload that license file. Ensure it is a valid file and try again. If the problem persists, please contact <a href="mailto:support@datatorrent.com">support@datatorrent.com</a>'
            })
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