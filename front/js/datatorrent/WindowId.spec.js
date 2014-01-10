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
var WindowId = require('./WindowId');

describe('WindowId.js', function() {
    
    var windowId;
    var windowValue = '5896637953039405386';
    var windowOffset = 4426;
    var windowTimestamp = 1372918010000;
    
    beforeEach(function() {
        windowId = new WindowId(windowValue);
    });
    
    afterEach(function() {
        windowId = undefined;
    });
    
    it('should be a function', function() {
        expect(WindowId).to.be.a('function');
    });

    it('timestamp should be a Date', function() {
        expect(windowId.timestamp).to.be.instanceof(Date);
    });
    
    it('should have timestamp, offset, and value', function() {
        expect( windowId.timestamp.valueOf() ).to.equal(windowTimestamp);
        expect(windowId.offset).to.equal(windowOffset);
        expect(windowId.value).to.equal(windowValue);
    });
    
    it('should not throw if no arguments passed', function() {
        var fn = function() {
            return new WindowId();
        }
        expect(fn).not.to.throw(Error);
    });
    
    it('should not throw even if first arg is non-numeric string', function() {
        var fn = function() {
            return new WindowId('343k18ia');
        }
        expect(fn).not.to.throw(Error);
    });

    it('toString should be a span with a tooltip and "invalid" as content', function() {
        var w = new WindowId('2398ksdfj');
        var $el = $(w.toString());
        expect($el.text()).to.equal('Invalid');
        expect($el.attr('title')).to.match(/2398ksdfj/);
    });
    
    it('should have a set method that allows you to change the value', function() {
        expect(windowId.set).to.be.a('function');
        windowId.set('5896637953039405387');
        expect(windowId.timestamp.valueOf()).to.equal(1372918010000);
        expect(windowId.offset).to.equal(4427);
        expect(windowId.value).to.equal('5896637953039405387');
    });

    describe('a windowId with an initial value of 0 or -1', function() {
        
        var w0, w1;

        beforeEach(function() {
            w0 = new WindowId('0');
            w1 = new WindowId('-1');
        });

        afterEach(function() {
            w0 = w1 = null;
        });

        it('should return "-" from its toString method', function() {
            expect(w0.toString()).to.equal('-');
            expect(w1.toString()).to.equal('-');
        });

    });

});