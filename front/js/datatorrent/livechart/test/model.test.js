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
var Model = require('../lib/LiveChartModel');

describe('the livechart model', function() {
    
    var sandbox;
    
    beforeEach(function() {
        sandbox = sinon.sandbox.create();
    });
    
    afterEach(function() {
        sandbox.restore();
    });
    
    describe('initialize method', function() {
        
        var i;
        
        beforeEach(function() {
            sandbox.spy(Model.prototype, 'state');
            i = new Model({
                time_key: 'x',
                data: [
                    { x: 18, y: 20 }
                ], 
                plots: [
                    { key: "key1", color: "red", label: "Key 1" },
                    { key: "key2", color: "green", label: "Key 2" },
                    { key: "key3", color: "blue", label: "Key 3" },
                ]
            });
        });
        
        afterEach(function() {
            i = null;
        });
        
        
        it('should create a data collection', function() {
            expect(i.get('data')).to.be.instanceof(Backbone.Collection);
        });
        
        it('should use data passed to the attributes for the data collection', function() {
            expect(i.get('data').length).to.equal(1);
            expect(i.get('data').at(0).toJSON()).to.eql({
                x: 18,
                y: 20
            });
        });
        
        it('should set up a plot collection', function() {
            expect(i.plots).to.be.instanceof(Backbone.Collection);
        });
        
        it('should use plots passed to the attributes for the plot collection', function() {
            expect(i.plots.length).to.equal(3);
            expect(i.plots.pluck('key')).to.eql(['key1','key2','key3']);
        });
        
        it('should not look for previous state if no id is provided', function() {
            expect(i.state).not.to.have.been.calledOnce;
        });
        
        it('should look for previous state if an id is provided', function() {
            var k = new Model({
                id: 'tmp'
            });
            expect(k.state).to.have.been.calledOnce;
        });
        
    });
    
    
    describe('plot method', function() {
        
        var i;
        
        beforeEach(function() {
            i = new Model({});
        });
        
        afterEach(function() {
            i = undefined;
        });
        
        it('should add a plot to the plots collection', function() {
            var len = i.plots.length + 1;
            i.plot({
                'key': 'key1',
                'color': '#FF0000',
                'label': 'label1'
            });
            expect(i.plots.length).to.equal(len);
        });
        
        it('should set the label to the key if no label is provided', function() {
            i.plot({
                'key': 'key1',
                'color': '#CC0000'
            });
            var plot = i.plots.get('key1');
            expect(plot.get('label')).to.equal('key1');
        });
        
        it('should add in visibility by default if not present', function() {
            i.plot({
                'key': 'key1',
                'color': '#CC0000'
            });
            var plot = i.plots.get('key1');
            expect(plot.get('visible')).to.equal(true);
        });
        
        it('should be able to accept an array of plot objects', function() {
            var plots = [
                { key: 'key1', color: '#FF0000', label: 'Key 1' },
                { key: 'key2', color: '#00FF00' },
                { key: 'key3', color: '#0000FF', label: 'Key 3' }
            ];
            
            i.plot(plots);
            expect(i.plots.toJSON()).to.eql([
                { visible: true, key: 'key1', color: '#FF0000', label: 'Key 1' },
                { visible: true, key: 'key2', color: '#00FF00', label: 'key2' },
                { visible: true, key: 'key3', color: '#0000FF', label: 'Key 3' }
            ]);
        });
        
        it('should throw if no key is provided', function() {
            function fn() {
                i.plot({
                    nokey: 'is_bad',
                    color: '#ff0000',
                    label: 'bad_label'
                });
            }
            function fn2() {
                i.plot([
                    {
                        nokey: 'is_bad',
                        color: '#ff0000',
                        label: 'bad_label'
                    },
                    {
                        key: 'is_ok',
                        color: '#00ff00',
                        label: 'good_label'
                    }
                ]);
            }
            
            expect(fn).to.throw(TypeError);
            expect(fn2).to.throw(TypeError);
        });
        
    });
    
    describe('unplot method', function() {
        var i;
    
        beforeEach(function() {
            i = new Model({});
        });
    
        afterEach(function() {
            i = undefined;
        });
        
        it('should remove a plot object from the plots collection', function() {
            var plots = [
                { key: 'key1', color: '#FF0000', label: 'Key 1' },
                { key: 'key2', color: '#00FF00' },
                { key: 'key3', color: '#0000FF', label: 'Key 3' }
            ];
            
            i.plot(plots);
            i.unplot('key2');
            expect(i.plots.length).to.equal(2);
            expect(i.plots.pluck('key')).to.eql(['key1', 'key3']);
        });
        
    });
    
    describe('generateSeriesData method', function() {
        
        var i, sd;
        
        beforeEach(function() {
            i = new Model({
                data: [
                    { time: +new Date()-10000, key1: 1, key2: 2, key3: 3 },
                    { time: +new Date()-8000, key1: 2, key2: 1, key3: 2 },
                    { time: +new Date()-6000, key1: 3, key2: 2, key3: 1 },
                    { time: +new Date()-4000, key1: 4, key2: 3, key3: 0 },
                    { time: +new Date()-2000, key1: 5, key2: 2, key3: 1 }
                ],
                plots: [
                    { key: 'key1', color: '#FF0000' },
                    { key: 'key2', color: '#00FF00', visible: false },
                    { key: 'key3', color: '#0000FF' },
                ]
            });
            
            sd = i.generateSeriesData();
        });
        
        afterEach(function() {
            i = undefined;
        });
        
        it('should return an array of new series objects for each visible plot', function() {
            expect(sd.length).to.equal(2);
            expect(_.pluck(sd,'key')).to.eql(['key1','key3'])
        });
        
        it('should return objects with a plot, key, and values attributes', function() {
            _.each(sd, function(s) {
                expect(s).to.have.property('plot');
                expect(s).to.have.property('key');
                expect(s).to.have.property('values');
            })
        });
        
    });
    
    describe('loadStoredInfo method', function() {
        
        var state = {
            width: 800,
            height: 500,
            margin: {
                top: 0,
                right: 0,
                bottom: 40,
                left: 60
            }
        };
        
        beforeEach(function() {
            sandbox.stub(Model.prototype, 'state');
            Model.prototype.state.returns(state);
            sandbox.spy(Model.prototype, 'set');
        });
        
        afterEach(function() {
            sandbox.restore();
        });   
        
        it('should do nothing if no id is set', function() {
            var i = new Model({});
            i.loadStoredInfo();
            expect(i.state).not.to.have.been.called;
            expect(i.set).not.to.have.been.calledWith(state);
        });
        
        it('should call state and set if id is set', function() {
            var i = new Model({ id: 'tmp' });
            expect(i.state).to.have.been.calledOnce;
            expect(i.set).to.have.been.calledWith(state);
        });
        
        it('should pass the state to the set method with validate option on', function() {
            var i = new Model({ id: 'tmp' });
            var args = i.set.getCall(2).args;
            expect(args[0]).to.equal(state);
            expect(args[1]).to.eql({ validate: true });
        });
    });
    
});