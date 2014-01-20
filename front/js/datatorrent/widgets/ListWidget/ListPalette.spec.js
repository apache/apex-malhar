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
var Backbone = require('backbone');
var View = require('./ListPalette');

describe('ListPalette.js', function() {
    
    var sandbox;
    
    beforeEach(function() {
        sandbox = sinon.sandbox.create();
        sandbox.spy(View.prototype, 'render');
    });
    
    afterEach(function() {
        sandbox.restore();
    });
    
    it('should throw if a collection is not supplied', function() {
        var fn = function() {
            var v = new View({
                dataSource: dataSource
            });
        }
        expect(fn).to.throw();
    });
    
    it('should set the dataSource if it is available in the options', function() {
        var dataSource = {};
        var v = new View({
            collection: new Backbone.Collection([]),
            dataSource: dataSource
        });
        expect(v.dataSource).to.equal(dataSource);
    });
    
    it('should set the nav if it is available in the options', function() {
        var nav = {};
        var v = new View({
            collection: new Backbone.Collection([]),
            nav: nav
        });
        expect(v.nav).to.equal(nav);
    });
    
    it('should render when the change_selected event is triggered on its collection', function() {
        var data = new Backbone.Collection([]);
        var v = new View({
            collection: data
        });
        data.trigger('change_selected');
        expect(v.render).to.have.been.calledOnce;
    });
    
    describe('the getSelected method', function() {
        
        var data, v;
        
        beforeEach(function() {
            data = new Backbone.Collection([
                { id: '1', key1: 'testing1' },
                { id: '2', key1: 'testing2' },
                { id: '3', key1: 'testing3' }
            ]);
            v = new View({
                collection: data
            });
            data.at(0).selected = true;
            data.at(1).selected = true;
        });
        
        afterEach(function() {
            data = v = undefined;
        });
        
        it('should return an array of all the models that are selected', function() {
            expect(v.getSelected()).to.eql([
                data.at(0),
                data.at(1)
            ]);
        });
        
        it('should return pojos if true as past as the only argument', function() {
            expect(v.getSelected(true)).to.eql([
                data.at(0).toJSON(),
                data.at(1).toJSON()
            ]);
        });
        
    });
    
    describe('the getTemplateData method', function() {

        var v, template_data;
        
        beforeEach(function() {
            data = new Backbone.Collection([
                { id: '1', key1: 'testing1' },
                { id: '2', key1: 'testing2' },
                { id: '3', key1: 'testing3' }
            ]);
            
            v = new View({
                collection: data
            });
            
            data.at(0).selected = true;
            data.at(1).selected = true;
            
            template_data = v.getTemplateData();
        });
        
        afterEach(function() {
            v = template_data = undefined;
        });
        
        it('should return an object with a selected array', function() {
            var tds = template_data.selected;
            var vgs = v.getSelected();
            expect(tds.length).to.eq(vgs.length);
            expect(tds[0].id).to.eql(vgs[0].id);
            expect(tds[1].id).to.eql(vgs[1].id);
        });
        
    });
    
});