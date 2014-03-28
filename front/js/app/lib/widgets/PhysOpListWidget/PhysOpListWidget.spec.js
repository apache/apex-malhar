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
var Operator = DT.lib.OperatorModel;
var Widget = DT.widgets.Widget;
var Backbone = require('backbone');
var W = require('./PhysOpListWidget');
var columns = require('./columns');

describe('PhysOpListWidget', function() {
    
    var sandbox, w, options;

    beforeEach(function() {

        sandbox = sinon.sandbox.create();
        sandbox.stub(Widget.prototype, 'initialize', function() {});
        sandbox.stub(Widget.prototype, 'compId', function() { return ''; });

        options = {
            appId: 'application_0001',
            nav: {},
            operators: new Backbone.Collection([]),
            dataSource: {}
        };
        w = new W(options);
    });

    afterEach(function() {
        sandbox.restore();
        options = null;
    });

    it('should set appId, nav, ops, and dataSource from options', function() {
        expect(w.appId).to.equal(options.appId);
        expect(w.nav).to.equal(options.nav);
        expect(w.operators).to.equal(options.operators);
        expect(w.dataSource).to.equal(options.dataSource);
    });

    it('should set a tabled subview and a palette subview', function() {
        expect(w.subview('tabled')).to.be.instanceof(Backbone.View);
        expect(w.subview('palette')).to.be.instanceof(Backbone.View);
    });

    describe('the noLogicalOperatorLinks option/flag', function() {

        beforeEach(function() {
            options = {
                appId: 'application_0001',
                nav: {},
                operators: new Backbone.Collection([]),
                dataSource: {},
                noLogicalOperatorLinks: true
            };
            w = new W(options);
        });
        
        it('should remove the name column from columns if set to true', function() {
            var tabled = w.subview('tabled');
            var name_column = tabled.columns.get('name');
            expect(name_column).to.eql(undefined);
        });

        it('should NOT remove the name column if not set or set to falsy value', function() {
            options.noLogicalOperatorLinks = false;
            var w2 = new W(options);
            var tabled = w2.subview('tabled');
            var name_column = tabled.columns.get('name');
            expect(name_column).to.be.an('object');
        });

        it('should not alter the original columns array', function() {
            var name_column = _.find(columns, function(col) {
                return col.id === 'name';
            });
            expect(name_column).to.be.an('object');
        });

    });

    describe('the remove method', function() {
        
        it('should remove "selected" property from all operators', function() {
            options.operators.add(new Operator({}));
            options.operators.at(0).selected = true;
            w.remove();
            expect(options.operators.at(0).selected).not.to.equal(true);
        });

    });

});