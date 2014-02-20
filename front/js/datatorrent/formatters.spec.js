var _ = require('underscore');
var formatters = require('./formatters');

describe('formatters.js', function() {
    
    var sandbox;

    beforeEach(function() {
        sandbox = sinon.sandbox.create();
    });

    afterEach(function() {
        sandbox.restore();
    });

    describe('the API', function() {

        _.each([
            'containerFormatter',
            'windowFormatter',
            'windowOffsetFormatter',
            'statusClassFormatter',
            'logicalOpStatusFormatter',
            'percentageFormatter',
            'byteFormatter'
        ], function(method) {
            it('should expose the ' + method + ' method', function() {
                expect(formatters[method]).to.be.a('function');
            });
        }, this);

    });

    describe('the byteFormatter', function() {
        
        it('should convert a string to a fixed tenths position number', function() {
            expect(formatters.byteFormatter('2048')).to.equal('2.0 KB');
        });

        it('should throw if the second argument is not an available level', function() {
            var fn = function() {
                formatters.byteFormatter(2048, {});
            }
            expect(fn).to.throw();
        });

    });

});