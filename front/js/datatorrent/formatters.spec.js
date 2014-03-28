/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
var _ = require('underscore');
var formatters = require('./formatters');

describe('formatters.js', function() {
    
    var sandbox, f;

    beforeEach(function() {
        sandbox = sinon.sandbox.create();
    });

    afterEach(function() {
        sandbox.restore();
        f = undefined;
    });

    describe('the API', function() {

        _.each([
            'containerFormatter',
            'windowFormatter',
            'windowOffsetFormatter',
            'statusClassFormatter',
            'logicalOpStatusFormatter',
            'percentageFormatter',
            'byteFormatter',
            'cpusFormatter'
        ], function(method) {
            it('should expose the ' + method + ' method', function() {
                expect(formatters[method]).to.be.a('function');
            });
        }, this);

    });

    describe('the byteFormatter', function() {
        
        beforeEach(function() {
            f = formatters.byteFormatter;
        });

        it('should convert a string to a fixed tenths position number', function() {
            expect(f('2048')).to.equal('2.0 KB');
        });

        it('should throw if the second argument is not an available level', function() {
            var fn = function() {
                f(2048, {});
            }
            expect(fn).to.throw();
        });

    });

    describe('the cpusFormatter', function() {
        
        beforeEach(function() {
            f = formatters.cpusFormatter;
        });

        it('should take a percentage as a first argument and return a string', function() {
            expect(f(0.1)).to.be.a('string');
        });

        it('should convert to CPU count to .01 precision', function() {
            expect(f(1.1)).to.equal('1.10');
        });

        it('should convert percentage numerator to decimal if numerator flag is set', function() {
            expect(f(110, true)).to.equal('1.10');
        });

        it('should be able to take a string', function() {
            expect(f('1.1')).to.equal('1.10');
            expect(f('110', true)).to.equal('1.10');
        });

    });

});