var ModalView = require('./ModalView');

describe('ModalView.js', function() {
    
    var sandbox;

    beforeEach(function() {
        sandbox = sinon.sandbox.create();
        $.fn.modal = sandbox.spy();
    });

    afterEach(function() {
        sandbox.restore();
    });

    it('should pick up launchOptions from options on initialize', function() {
        var launchOptions = { backdrop: 'static' };
        var v = new ModalView({ launchOptions: launchOptions });
        expect(v.launchOptions).to.equal(launchOptions);
    });

    describe('the launch method', function() {

        it('should call $().modal', function() {
            var launchOptions = { backdrop: 'static' };
            var v = new ModalView({ launchOptions: launchOptions });
            v.launch();
            expect($.fn.modal).to.have.been.calledOnce;
        });

        it('should take options that override view.launchOptions', function() {
            var launchOptions = { backdrop: 'static', show: true };
            var v = new ModalView({ launchOptions: launchOptions });
            v.launch({ backdrop: true });
            expect($.fn.modal.getCall(0).args[0]).to.eql({ backdrop: true, show: true });
        });
        
    });

});