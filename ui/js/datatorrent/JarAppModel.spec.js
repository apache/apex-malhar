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
var settings = require('./settings');
var Col = require('./JarAppCollection');
var Model = require('./JarAppModel');
describe('JarAppModel.js', function() {
    
    var sandbox;
    
    beforeEach(function() {
        sandbox = sinon.sandbox.create();
    });
    
    afterEach(function() {
        sandbox.restore();
    });
    
    it('should set fileName from options', function() {
        var m = new Model({name: 'SomeAppName', fileName: 'some.jar'});
        expect(m.get('fileName')).to.equal('some.jar');
    });
    
    it('should use its collection fileName if it is present', function() {
        var c = new Col(
            [{name: "SomeAppName"}],
            {fileName: "other.jar"}
        );
        expect(c.at(0).get('fileName')).to.equal('other.jar');
    });
    
    describe('launch method', function() {
        
        // POST /ws/v1/jars/{jarfile}/applications/{appname}/launch
        // Function: Launch the application
        // Payload: 
        // {
        //     “{propertyName}” : “{propertyValue}”, ...
        // }
        // Return: 
        // {
        //     “appId”: “{appId}”
        // }
        // 
        
        it('should call $.ajax with the correct URL', function() {
            sandbox.stub($, 'ajax', function(options) {
                expect(options.url).to.equal(settings.interpolateParams(settings.actions.launchApp, {
                    v: 'v1',
                    fileName: 'some.jar',
                    appName: 'some%2FApp%2FName'
                }));
            });
            
            var m = new Model({name: "some/App/Name", fileName: "some.jar"});
            
            m.launch();
            expect($.ajax).to.have.been.calledOnce;
            
        });
        
    });
    
});