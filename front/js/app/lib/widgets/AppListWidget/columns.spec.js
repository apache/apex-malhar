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
    var columns = require('./columns');
var _ = require('underscore');
var Backbone = require('backbone');
describe('App List Columns', function() {
    
    var sandbox, colmap;

    before(function() {
        colmap = {};
        _.each(columns, function(col) {
            colmap[col.id] = col;
        });
    });

    beforeEach(function() {
        sandbox = sinon.sandbox.create();
    });

    afterEach(function() {
        sandbox.restore();
    });

    describe('the stateFormatter', function() {
        var formatter;

        beforeEach(function() {
            formatter = colmap.state.format;
        });

        it('should return a dash when a falsy value is passed', function() {
            _.each([false, null, undefined, '', 0], function(val) {
                expect(formatter(val)).to.equal('-');
            });
        });

        it('should create a span with correct status class', function() {
            var result = formatter('RUNNING', new Backbone.Model({}));
            expect(result).to.equal('<span class="status-running">RUNNING</span>');
        });

        it('should not create an extra span when the final status is UNDEFINED', function() {
            var result = formatter('RUNNING', new Backbone.Model({ finalStatus: 'UNDEFINED'}));
            expect(result).to.equal('<span class="status-running">RUNNING</span>'); 
        });

        it('should create an extra span when the final status is a string and not "UNDEFINED"', function() {
            var result = formatter('FAILED', new Backbone.Model({ finalStatus: 'KILLED'}));
            expect(result).to.equal('<span class="status-failed">FAILED</span> <span class="final-status" title="Final Status">(KILLED)</span>');
        });

    });

    describe('the stateOrder', function() {
        var sorter, orderArray;

        beforeEach(function() {
            sorter = colmap.state.sort;
            orderArray = DT.settings.statusOrder;
        });

        it('should return order values that coordinate with DT.statusOrder array', function() {
            var row1 = new Backbone.Model({ state: orderArray[0] });
            var row2 = new Backbone.Model({ state: orderArray[1] });
            expect(sorter(row1, row2)).to.be.below(0);

            row1 = new Backbone.Model({ state: orderArray[2] });
            row2 = new Backbone.Model({ state: orderArray[1] });
            expect(sorter(row1, row2)).to.be.above(0);
            expect(sorter(row1, row1)).to.equal(0);
        });
    });

    describe('the idFilter function', function() {

        var filter;

        beforeEach(function() {
            filter = colmap.id.filter;
        });

        it('should only filter on the last section of the id', function() {
            expect(filter('59', 'application_98305909832_0001')).to.equal(false);
            expect(filter('59', 'application_98305909832_0059')).to.equal(true);
        });
    });

    describe('the memoryFormatter', function() {
        var formatter;
        beforeEach(function() {
            formatter = colmap.allocatedMB.format;
        });

        it('should return a dash when a falsy value is passed', function() {
            _.each([false, null, undefined, '', 0], function(val) {
                expect(formatter(val)).to.equal('-');
            });
        });
    });

    describe('the memorySorter', function() {
        var sorter;
        beforeEach(function() {
            sorter = colmap.allocatedMB.sort;
        });

        it('should return zero if both rows have undefined or falsy allocatedMB', function() {
            var v1 = sorter(new Backbone.Model({}), new Backbone.Model({}));
            expect(v1).to.equal(0);
            v1 = sorter(new Backbone.Model({ allocatedMB: false }), new Backbone.Model({ allocatedMB: false }));
            expect(v1).to.equal(0);
            v1 = sorter(new Backbone.Model({ allocatedMB: '' }), new Backbone.Model({ allocatedMB: '' }));
            expect(v1).to.equal(0);
            v1 = sorter(new Backbone.Model({ allocatedMB: null }), new Backbone.Model({ allocatedMB: null }));
            expect(v1).to.equal(0);
        });

        it('should rate undefined/falsy values as lower than those with memory', function() {
            var v1 = sorter(new Backbone.Model({}), new Backbone.Model({ allocatedMB: 10 }));
            expect(v1).to.be.below(0);
        });

        it('should rate apps using more memory higher than those using less', function() {
            var v1 = sorter(new Backbone.Model({ allocatedMB: 19 }), new Backbone.Model({ allocatedMB: 10 }));
            expect(v1).to.be.above(0);
            v1 = sorter(new Backbone.Model({ allocatedMB: 9 }), new Backbone.Model({ allocatedMB: 10 }));
            expect(v1).to.be.below(0);
        });

    });

});