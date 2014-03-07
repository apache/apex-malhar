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
var Notifier = require('../Notifier');
var path = require('path');
var _ = require('underscore'), Backbone = require('backbone');
var HTTP_VERBS = ['GET', 'POST', 'PUT', 'DELETE'];
var settings = require('../settings');
var visibly = require('../../vendor/visibly.polyfill');

// CONSTRUCTOR
function DataSource(host, user) {
    
    // Check for valid args
    if (!host || typeof host !== 'string') {
        throw new Error('DataSource must be given a host name to connect to');
    }
    if (!user || !(user instanceof Backbone.Model)) {
        throw new Error('DataSource must be given a user object (Backbone model)');
    }
    
    // Mixin backbone events
    _.extend(this, Backbone.Events);
    
    // This is the version of the jetty daemon
    this.version = settings.version;
    
    // This is the url of the host without a 
    // protocol but with port (eg. www.example.com:9000)
    this.host = host;

    // Store the user object for use later
    this.user = user;
}

DataSource.prototype = {

    connect: function() {
        // Pointer to this instance
        var self = this, isHidden = visibly.hidden(), visibleTimer;

        visibly.onHidden(function() {
            visibleTimer = setTimeout(function() {
                LOG('3', 'Console hidden for more than ' + Math.round(settings.visibility_timeout/1000) + ' seconds. Turning off WebSocket messages.');
                isHidden = true;
            }, settings.visibility_timeout);
        });

        visibly.onVisible(function() {
            clearTimeout(visibleTimer);
            isHidden = false;
        });

        // This is the WebSocket object. A deferred is
        // created so the class can determine if the
        // ws connection has been established.
        LOG('1', 'WebSocket connection attempt.', ['host: ', this.host, 'url: ', 'ws://'+this.host+'/pubsub'])
        this.ws = new WebSocket('ws://'+this.host+'/pubsub');
        this.__wsCxn = $.Deferred();
        this.ws.onopen = function(evt){
            LOG('2', 'WebSocket connection established');
            self.__wsCxn.resolveWith( self, [self.ws] );
        }
        this.supressOnClose = false;
        this.ws.onclose = function() {
            LOG('3', 'WebSocket connection has closed');
            if (!self.supressOnClose) {
                Notifier.warning({
                    'title': 'WebSocket connection closed',
                    'text': 'The WebSocket connection to the Gateway has been closed. You may have left the network or the server itself may be down. Try <a href="Javascript:window.location.reload(true)">refreshing the page</a>.',
                    'hide': false
                });
            }
        }
        this.ws.onmessage = function(msg) {
            var msgJson;

            if (isHidden) {
                return;
            }

            try {
                // to parse the json
                msgJson = JSON.parse(msg.data);
                
            } catch(e) {
                
                self.trigger('error', {
                    title: 'Error receiving data from server',
                    text: e.message,
                    trace: e.trace,
                    data: msg.data
                });
                
                

            }
            
            // #DEV
            if (window.WS_DEBUG) {
                LOG(6, 'WebSocket', [msgJson.topic + ': ', msgJson.data]);
            }
            // /DEV
            
            // check the format of the message
            if ( !msgJson || !msgJson.hasOwnProperty('type') || !msgJson.hasOwnProperty('data') || !msgJson.hasOwnProperty('topic') ) {
                // Ensure msgJson is an object
                msgJson = msgJson || {};
                // Check for a topic
                var topic = msgJson.hasOwnProperty('topic') ? msgJson.topic : false ;
                if (topic) {
                    self.unsubscribe(topic);
                } else {
                    self.ws.close();
                }
                
                // Notify
                Notifier.error({
                    title: 'Malformed Web Socket data received',
                    text: 'Information was received from the server that was not in the expected format.'
                });
                
                // trigger a notifier error
                return;
            }
            
            // trigger to listeners
            self.trigger(msgJson.topic, msgJson.data);
            // LOG(2, 'Received publish from topic: ', [msgJson.topic, '; data: ', msgJson.data]);

            // lazy unsubscribe
            if (self._events === undefined || !self._events.hasOwnProperty(msgJson.topic)) {
                self.unsubscribe(msgJson.topic);
            }

        }
        this.ws.onerror = function(evt){
            LOG('4', 'WebSocket error occurred');
            Notifier.error({
                'title': 'A WebSocket error occurred',
                'text': 'A WebSocket connection could not be established. If you are using browser proxy settings to connect, ensure that these are correct.',
                'hide': false
            });
        }
    },
    disconnect: function () {
        this.__wsCxn.done(function () {
            this.supressOnClose = true;
            this.ws.close();
        })
    },
    _ajax: function(type, options) {
        if (!options.url || typeof options.url !== 'string') {
            throw new Error('The url option is required for all ajax methods');
        }
        options.type = type;
        return $.ajax(options);
    },
    get: function(options){
        return this._ajax('GET', options);
    },
    post: function(options){
        if (options.data) {
            options.data = JSON.stringify(options.data);
            options.contentType = 'application/json; charset=utf-8';
        }
        return this._ajax('POST', options);
    },
    put: function(options){
        return this._ajax('PUT', options);
    },
    delete: function(options){
        return this._ajax('DELETE', options);
    },
    _send: function(message) {
        var self, fn, state;
        
        message = JSON.stringify(message);
        
        self = this;
        
        fn = function(){
            self.ws.send(message);
        }
        
        state = this.__wsCxn.state();
        
        if (state == 'resolved') {
            fn();
        } else if (state == 'pending') {
            self.__wsCxn.done(fn)
        }
    },
    subscribe: function(topic) {
        var message = { 'type':'subscribe', 'topic': topic };
        this._send(message);
        LOG(1, 'subscribe', ['topic:', topic]);
    },
    unsubscribe: function(topic) {
        var message = {'type':'unsubscribe', 'topic': topic};
        this._send(message);
        LOG(2, 'unsubscribe', ['topic:', topic]);
    },
    // launchApp: function(options) {
    //     
    //     checkSingleArg(arguments, options, 'launchApp');
    //     
    //     // Ensure a data object
    //     options.data = options.data || {};
    //     
    //     // 
    // },
    shutdownApp: function(options) {
        return this._endApp('shutdown', options);
    },
    killApp: function(options) {
        return this._endApp('kill', options);
    },
    _endApp: function(method, options) {
        // Ensure that appId is present
        if (!options.appId) {
            throw new Error('The ' + method + 'App method requires an appId');
        }
        
        var appId = options.appId;
        delete options.appId;
        
        // Set the url
        options.url = settings.interpolateParams(settings.actions[method + 'App'], {
            appId: appId,
            v: settings.version
        });
        
        // Default error function
        if (typeof options.error !== 'function') {
            options.error = function() {
                Notifier.error({
                    'title': 'Request to ' + method + ' application failed',
                    'text': 'An error occurred trying to ' + method + ' ' + appId + '.'
                });
            }
        }
        
        // Default success function
        if (typeof options.success !== 'function') {
            options.success = function() {
                Notifier.success({
                    'title': 'Request to ' + method + ' application received',
                    'text': 'The server has received your request. It may take a few moments for the application to end.'
                });
            }
        }
        this.post(options);
    },
    getRecordingTuples: function(options){
        // Set up variables from options
        var appId = options.appId, 
            operatorId = options.operatorId, 
            startTime = options.startTime, 
            limit = options.limit, 
            data = {};
        
        checkSingleArg(arguments, options, 'getRecordingTuples');
        
        // Check for required info
        if (!appId || !operatorId || !startTime || !limit) {
            throw new Error('getRecordingTuples requires appId, operatorId, limit, and startTime')
        } else {
            delete options.appId;
            delete options.operatorId;
            delete options.startTime;
            delete options.limit;
            data.limit = limit;
        }
        
        // Make sure ports is comma separated
        if (options.ports) {
            if (options.ports instanceof Array) {
                data.ports = options.ports.join(',');
                delete options.ports;
            } else {
                throw new Error('If ports are specified by options in getRecordingTuples, it must be an array');
            }
        }
        
        // Check for offset or startWindow problems.
        // Strictly check for undefined since 0 is a falsey but acceptable value
        if (options.offset === undefined && options.startWindow === undefined) {
            throw new Error('Neither offset nor startWindow was supplied to getRecordingTuples.');
        }
        if (options.offset !== undefined && options.startWindow !== undefined) {
            throw new Error('Both offset and startWindow cannot be present in getRecordingTuples.');
        }
        
        // Add offset or startWindow to data
        if (options.offset) {
            data.offset = options.offset;
        }
        if (options.startWindow) {
            data.startWindow = options.startWindow;
        }
        delete options.offset;
        delete options.startWindow;
        
        // Set up url to request
        options.url = settings.interpolateParams(settings.urls.RecordingTuples, {
            appId: appId, 
            operatorId: operatorId, 
            startTime: startTime+'',
            v: settings.version
        });
        
        // Set default error function
        if (typeof options.error !== 'function') {
            options.error = function(xhr, textStatus, errorThrown) {
                var emsg = 'Tuples failed to load.';
                Notifier.error({
                    'title': emsg,
                    'text': 'The tuples requested could not be retrieved.'
                });
                throw new Error( emsg + ' URL: ' + options.url);
            }
        }
        
        // Attach data to options
        options.data = data;
        
        // Execute the request
        this.get(options);
        
        LOG(1, 'getting tuples', ['url:', options.url, 'appId: ', appId, ', operatorId: ', operatorId, ', startTime: ', startTime, ', options: ', options]);
    },
    getAlertClasses: function(options) {
        var successCallback;
        
        checkSingleArg(arguments, options, 'getAlertClasses');
        
        // Require appId and classType
        if (!options.appId || !options.classType) {
            throw new Error('appId and classType are required for getAlertClasses');
        }
        
        // Make url
        options.url = path.join('/alerts', this.version, options.appId, options.classType + 'Classes');
        
        // Set default error handler if not present
        if (!options.error) {
            options.error = function(xhr, textStatus, errorThrown) {
                var emsg = 'Alert classes failed to load.';
                Notifier.error({
                    'title': emsg,
                    'text': 'An error occurred retrieving ' + options.classType + ' alert classes.'
                });
                throw new Error( emsg + ' URL: ' + options.url);
            }
        }
        
        // #DEV
        successCallback = options.success;
        options.success = function(res, textStatus, xhr) {
            // Check for correct format from Daemon
            if (!res.classes) {
                var emsg = 'Alert classes received not in the expected format';
                LOG(4, emsg, res);
                // Display error and return
                Notifier.error({
                    title: 'Unexpected format from getAlertClasses',
                    text: emsg
                });
                
                return;
            }

            // If success callback was provided, invoke it
            if (typeof successCallback === 'function') {
                successCallback(res.classes, textStatus, xhr);
            }
            
            LOG(2, 'received alert classes: ', res);
                    
        }
        // /DEV
        LOG(1, 'getting alert classes', ['appId: ', options.appId, ' classType: ', options.classType, 'URL: ', options.url]);
        this.get(options);
    },
    getOperatorClass: function(options) {
        var successCallback;
        
        checkSingleArg(arguments, options, 'getOperatorClass');
        
        // Require appId and classType
        if (!options.appId || !options.className) {
            throw new Error('appId and className are required for getOperatorClass');
        }
        
        // Set the url
        options.url = path.join('/stram', this.version, 'applications', options.appId, 'describeOperator');
        
        // Add className to the options
        options.data = {
            'class': options.className
        }
        
        // Set default error handler if not present
        if (!options.error) {
            options.error = function(xhr, textStatus, errorThrown) {
                var emsg = 'Operator class failed to load.';
                Notifier.error({
                    'title': emsg,
                    'text': 'An error occurred retrieving ' + options.className + ' class.'
                });
                throw new Error( emsg + ' URL: ' + options.url);
            }
        }
        
        successCallback = options.success;
        options.success = function(res, textStatus, xhr) {
            // Check for correct format from Daemon
            if (!res.inputPorts && !res.outputPorts && !res.properties) {
                var emsg = 'Error loading operator class';
                LOG(4, emsg, res);
                // Display error and return
                Notifier.error({
                    title: 'Unexpected format from getOperatorClass',
                    text: emsg
                });
                
                return;
            }
        
            // If success callback was provided, invoke it
            if (typeof successCallback === 'function') {
                successCallback(res, textStatus, xhr);
            }
            
            LOG(2, 'received operator class: ', res);
                    
        }
        
        LOG(1, 'getting operator class', ['appId: ', options.appId, ' className: ', options.className, ' data: ', ])
        this.get(options);
        
        
    },
    startOpRecording: function(options) {
        // Ensure there is only one arg
        if (arguments.length !== 1 || typeof options !== 'object') {
            throw new Error('startOpRecording requires exactly one argument; an options object.');
        }
        return this._actOnOpRecording('start', options);
    },
    stopOpRecording: function(options) {
        // Ensure there is only one arg
        if (arguments.length !== 1 || typeof options !== 'object') {
            throw new Error('stopOpRecording requires exactly one argument; an options object.');
        }
        return this._actOnOpRecording('stop', options);
    },
    _actOnOpRecording: function(action, options) {
        
        // Ensure that appId and operatorId is present
        if (!options.appId || !options.operatorId) {
            throw new Error(action + 'OpRecording requires an appId and an operatorId');
        }
        
        // Ensure that version is in options for url
        options.v = settings.version;
        
        // Set the url
        var suffix = options.portName === undefined ? 'OpRecording' : 'PortRecording';
        options.url = settings.interpolateParams(settings.actions[action + suffix], options);
        
        // Default error function
        if (typeof options.error !== 'function') {
            options.error = function(res) {
                Notifier.error({
                    'title': 'Request to ' + action + ' recording failed',
                    'text': 'An error occurred trying to ' + action + ' recording this operator.'
                });
            }
        }
        
        // Default success function
        if (typeof options.success !== 'function') {
            options.success = function() {
                Notifier.success({
                    'title': 'Request to ' + action + ' recording received',
                    'text': 'The server has received your request. It may take a few moments to reflect recording status. If the recording does not ' + action + ', this may be an issue with the server.'
                });
            }
        }
        
        this.post(options);
        LOG(1, action + ' recording', [options]);
    },
    requestLicense: function (params) {
        var url = settings.interpolateParams(settings.urls.LicenseRequest, {
            v: settings.version
        });

        var options = {
            data: params,
            url: url
        }

        //var d = $.Deferred();
        //d.resolve();
        //return d.promise();

        return this.post(options);
    },
    getLicenseLastRequest: function (params) {
        var url = settings.interpolateParams(settings.urls.LicenseLastRequest, {
            v: settings.version
        });

        var options = {
            data: params,
            url: url
        }

        return this.get(options);
    },
    getConfigIPAddresses: function () {
        var url = settings.interpolateParams(settings.urls.ConfigIPAddresses, {
            v: settings.version
        });

        var options = {
            url: url
        }

        return this.get(options);
    }
}

function checkSingleArg(args,options,methodName) {
    // Ensure there is only one arg
    if (args.length !== 1 || typeof options !== 'object') {
        throw new Error(methodName + ' requires exactly one argument; an options object.');
    }
}

exports = module.exports = DataSource;