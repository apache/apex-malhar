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
/**
 * Application
 * 
 * Models an instance of a DataTorrent application
*/
var _ = require('underscore'), Backbone = require('backbone');
var BigInteger = require('jsbn');
var BaseModel = require('./BaseModel');
var bormat = require('bormat');
var LogicalPlan = require('./LogicalPlanModel');
var PhysicalPlan = require('./PhysicalPlanModel');
var Operators = require('./OperatorCollection');
var Containers = require('./ContainerCollection');
var WindowId = require('./WindowId');
var loadInfoAttempts = 1;
var loadInfoTimeout;
var ApplicationModel = BaseModel.extend({
    
    debugName: 'application',
    
    defaults: {
        'id': '',
        'appId': '',
        'name': '',
        'allocatedMB': undefined,
        'appMasterTrackingUrl': '',
        'appPath': '',
        'daemonAddress': '',
        'elapsedTime': 0,
        'recoveryWindowId': new WindowId('0'),
        'currentWindowId': new WindowId('0'),
        'stats': {
            'allocatedContainers': '',
            'criticalPath': [],
            'currentWindowId': '0',
            'failedContainers': '',
            'latency': '',
            'numOperators': '',
            'plannedContainers': '',
            'recoveryWindowId': '0',
            'totalBufferServerReadBytesPSMA': '',
            'totalBufferServerWriteBytesPSMA': '',
            'totalMemoryAllocated': '',
            'totalTuplesEmitted': '',
            'totalTuplesEmitted_f': '',
            'totalTuplesProcessed': '',
            'totalTuplesProcessed_f': '',
            'tuplesEmittedPSMA': '',
            'tuplesEmittedPSMA_f': '',
            'tuplesProcessedPSMA': '',
            'tuplesProcessedPSMA_f': ''
        },
        'user': '',
        'version': '',
        'amContainerLogs': '',
        'amHostHttpAddress': '',
        'clusterId': '',
        'diagnostics': '',
        'finalStatus': '',
        'finishedTime': '',
        'progress': '',
        'queue': '',
        'startedTime': false,
        'state': 'UNKNOWN',
        'trackingUI': '',
        'trackingUrl': '',
        'logicalPlan': undefined
    },
    
    initialize: function(attributes, options) {
        this.dataSource = options.dataSource;
        
        this.on('change:state', function() {
            var state = this.get('state');
            if (['KILLED', 'FINISHED', 'FAILED'].indexOf(state) !== -1) {
                this.unsubscribe();
                this.setOperators([]);
                this.setContainers([]);
                this.operators.trigger('reset');
                this.containers.trigger('reset');
            }
        });
    },
    
    urlRoot: function() {
        return this.resourceURL('Application');
    },
    
    fetch: function(options) {
        options = options || {};
        var oldSuccess = options.success;
        options.success = function(model, response, actions) {
            if (typeof oldSuccess === 'function') {
                oldSuccess.apply(this, _.toArray(arguments));
            }
            // Fetch operators and containers if they are set and this app is running
            if (model.get('state').toLowerCase() === 'running') {
                _.each(['operators', 'containers'], function(key) {
                    if (this[key]) {
                        this[key].fetch();
                    }
                }, model);
            }
        };
        return BaseModel.prototype.fetch.call(this, options);
    },
    
    serialize: function(noFormat){
        
        var obj = this.toJSON();
        
        // Calculate timestamp
        var lastHeartbeat = new Date(obj.elapsedTime * 1 + obj.startedTime * 1 );
        obj.lastHeartbeat = +lastHeartbeat;
        
        // Calculate allocated memory across all containers
        if (this.containers) {
            obj.totalAllocatedMemory = this.containers.reduce(function(memo, ctnr) {
                return memo + ctnr.get('memoryMBAllocated') * 1;
            }, 0);
            obj.totalAllocatedMemory = Math.round( (obj.totalAllocatedMemory * 100) / 1024) / 100;
        }
        
        // Check for additional formatting
        if ( ! noFormat ) {
            
            // Additional data
            if (obj.lastHeartbeat !== 0) {
                obj.as_of = (+new Date() - lastHeartbeat) < 24 * 60 * 60 * 1000 ? lastHeartbeat.toLocaleTimeString() : lastHeartbeat.toLocaleString();
            } else {
                obj.as_of = '-';
            }
            obj.up_for = bormat.timeSince(undefined, { timeChunk: obj.elapsedTime*1, unixUptime: true });
            obj.idShorthand = obj.id.split('_')[2];
            
            // Make comma group formatting
            _.each(['totalTuplesEmitted','tuplesEmittedPSMA','totalTuplesProcessed','tuplesProcessedPSMA'], function(key){
                obj.stats[key + '_f'] = bormat.commaGroups(obj.stats[key]);
            });
        }
        
        return obj;
    },
    
    setOperators: function(ops) {
        // Add appId to every one
        _.each(ops, function(op) {
            op.appId = this.get('id');
        }, this);
        
        // Only create new collection if necessary
        if (!this.operators) {
            this.operators = new Operators([], {
                dataSource: this.dataSource,
                appId: this.get('id')
            });
        }
        this.operators.set(ops);
        
        return this;
    },
    
    setContainers: function(ctnrs) {
        // Add appId to every one
        _.each(ctnrs, function(ctnr) {
            ctnr.appId = this.get('id');
        }, this);
        
        // Only create new collection if necessary
        if (!this.containers) {
            this.containers = new Containers([], {
                dataSource: this.dataSource,
                appId: this.get('id')
            });
            this.listenTo(this.containers, 'update', this.updateCtnrAggValues);
        }
        this.containers.set(ctnrs);
        
        return this;
    },
    
    unsetOperators: function() {
        if (this.operators) {
            this.operators.stopListening();
            this.stopListening(this.operators);
            this.operators = undefined;
            delete this.operators;
        }
    },
    
    unsetContainers: function() {
        if (this.containers) {
            this.containers.stopListening();
            this.stopListening(this.containers);
            this.containers = undefined;
            delete this.containers;
        }
    },
    
    subscribe: function() {
        
        var topic = this.resourceTopic('Application', { appId: this.get('id') }),
            state = this.get('state');
        
        this.checkForDataSource();
        this.listenTo(this.dataSource, topic, function(stats) {
            var updates = {};

            // Move attributes to main object where applicable
            _.each(['recoveryWindowId', 'currentWindowId', 'state'], function(key) {
                updates[key] = stats[key];
                delete stats[key];
            }, this);
            
            updates.stats = stats;
            var lcState = updates.state.toLowerCase();
            if ( lcState === 'running' || lcState === 'accepted' ) {
                updates.elapsedTime = +new Date() - 1 * this.get('startedTime');
            }
            this.set(updates);
        });
        this.dataSource.subscribe(topic);
        
        // Only subscribe to operators and containers if this app is running
        var subscribeToOpsAndCtnrs = _.bind(function() {
            if (this.operators) {
                this.operators.subscribe();
            }

            if (this.containers) {
                this.containers.subscribe();
            }
            
        }, this);
        
        // Immediately, or when the application starts running
        if (state === 'RUNNING') {
            subscribeToOpsAndCtnrs();
        } else {
            this.once('change:state', function() {
                if (this.get('state') === 'RUNNING') {
                    subscribeToOpsAndCtnrs();
                }
            });
        }
        return this;
    },
    
    unsubscribe: function() {
        this.stopListening(this.dataSource, 'apps.list');
        this.stopListening(this.dataSource, 'apps.' + this.get('id'));
        if (this.operators) {
            this.operators.unsubscribe();
        }

        if (this.containers) {
            this.containers.unsubscribe();
        }
    },
    
    getStreams: function() {
        var streams = [];
        var appId = this.get('id');
        var logicalPlan = this.get('logicalPlan');
        if (!logicalPlan) {
            // Get logical plan
            this.loadLogicalPlan();
            return streams;
        }
        streams = _.map(logicalPlan.get('streams'), function(streamObj, streamName) {
            streamObj.appId = appId;
            return streamObj;
        });
        return streams;
    },
    
    getStream: function(streamName) {
        var streams = this.getStreams();
        var appId = this.get('id');
        if (!streams.length) {
            return {
                name: streamName,
                appId: appId
            };
        }
        return _.find(streams, function(stream) {
            return stream.name === streamName;
        });
    },
    
    loadLogicalPlan: function(options) {

        options = options || {};

        var plan;

        if (!this.logicalPlan) {
            plan = new LogicalPlan({},{appId: this.get('id')});
            this.set('logicalPlan', plan);
            this.listenTo(plan, 'sync', function() {
                this.trigger('change:logicalPlan');
            });
        } else {
            plan = this.logicalPlan;
        }
        
        switch(this.get('state').toLowerCase()) {
            case 'accepted':
                this.once('change:state', function(model, state) {
                    if (state.toLowerCase() === 'running') {
                        plan.fetch(options);
                    }
                });
            break;
            
            case 'running':
                plan.fetch(options);
            break;
        }
    },

    loadPhysicalPlan: function(options) {
        options = options || {};
        var plan = new PhysicalPlan({},{appId: this.get('id')});
        plan.fetch(options);
    },
    
    shutdown: function(dataSource, noConfirm) {
        dataSource = dataSource || this.dataSource;
        var c = noConfirm || confirm('Are you sure you want to shutdown this application?');
        if (c) {
            dataSource.shutdownApp({
                appId: this.get('id'),
                complete: _.bind(function() {
                    this.fetch();
                }, this)
            });
        }
    },
    
    kill: function(dataSource, noConfirm) {
        dataSource = dataSource || this.dataSource;
        var c = noConfirm || confirm('Are you sure you want to kill this application?');
        if (c) {
            dataSource.killApp({
                appId: this.get('id'),
                complete: _.bind(function() {
                    this.fetch();
                }, this)
            });
        }
    },
    
    cleanUp: function() {
        this.off();
        this.stopListening();
        this.unsetOperators();
        this.unsetContainers();
        clearTimeout(loadInfoTimeout);
    }
    
});

exports = module.exports = ApplicationModel;