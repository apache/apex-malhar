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
var BaseModel = require('./BaseModel');
var BigInteger = require('jsbn');
var OperatorCollection = require('./OperatorCollection');
var bormat = require('bormat');
var WindowId = require('./WindowId');
var Notifier = require('./Notifier');
var text = require('./text');

/**
 * Model for containers
*/
// host: "node5.morado.com"
// id: "container_1367286541553_0128_01_000035"
// jvmName: "11906@node5.morado.com"
// lastHeartbeat: "1368134861013"
// memoryMBAllocated: "16384"
// memoryMBFree: "1691"
// numOperators: "2"
// state: "ACTIVE"
var ContainerModel = BaseModel.extend({
    
    debugName: 'container',

    defaults: function() {
        return {
            appId: '',
            host: '',
            id: '',
            jvmName: '',
            lastHeartbeat: '',
            memoryMBAllocated: 0,
            memoryMBFree: 0,
            numOperators: 0,
            state: '',
            recoveryWindowId_f: new WindowId('0'),
            currentWindowId_f: new WindowId('0')
        }
    },
    
    urlRoot: function() {
        return this.resourceURL('Container', {
            appId: this.get('appId')
        });
    },
    
    serialize: function(noFormat) {
        var json = this.toJSON();
        
        if (!noFormat) {
            json.as_of = new Date(json.lastHeartbeat*1).toLocaleString();
            json.totalTuplesEmitted_f = bormat.commaGroups(json.totalTuplesEmitted.toString());
            json.totalTuplesProcessed_f = bormat.commaGroups(json.totalTuplesProcessed.toString());
            json.tuplesEmittedPSMA_f = bormat.commaGroups(json.tuplesEmittedPSMA.toString());
            json.tuplesProcessedPSMA_f = bormat.commaGroups(json.tuplesProcessedPSMA.toString());
        }
        if (!json.containerLogsUrl) {
            json.containerLogsUrl = false;
        }
        
        return json;
    },
    
    setOperators: function(ops) {
        // Only create new collection if necessary
        if (!this.operators) {
            // Create the new collection
            this.operators = new OperatorCollection([], {
                dataSource: this.dataSource,
                appId: this.get('appId'),
                containerId: this.get('id')
            });
            
            // Set listener for updating aggregate values
            this.listenTo(this.operators, 'update', this.updateOpAggValues);
            
            // Update newly created collection with rows
            this.operators.set(ops);
        }
        
        // Update existing collection
        this.operators.set(ops);
    },
    
    updateOpAggValues: function() {
        var ops = this.operators.toJSON(false);
        var aggregates = _.reduce(
            ops,
            function(memo, op){
                // Windows
                _.each(['recoveryWindowId','currentWindowId'], function(key) {
                    if (memo[key] === false) {
                        memo[key] = op[key];
                    }
                    else {
                        if (op[key].offset*1 < memo[key].offset*1) {
                            memo[key] = op[key];
                        }
                    }
                });

                // Quantities
                _.each(['tuplesProcessedPSMA','totalTuplesProcessed','tuplesEmittedPSMA','totalTuplesEmitted'], function(key) {
                    memo[key] = memo[key].add(new BigInteger(op[key]+''));
                });
                
                // lastHeartbeat
                memo.lastHeartbeat = op.lastHeartbeat;
                
                return memo;
            },
            {
                'currentWindowId': false,
                'recoveryWindowId': false,
                'tuplesProcessedPSMA': BigInteger.ZERO,
                'totalTuplesProcessed': BigInteger.ZERO,
                'tuplesEmittedPSMA': BigInteger.ZERO,
                'totalTuplesEmitted': BigInteger.ZERO
            }
        );

        _.each(['recoveryWindowId', 'currentWindowId'], function(wKey) {
            if (aggregates[wKey] === false) {
                aggregates[wKey] = '0';
            }
        });

        this.set(aggregates);
    },
    
    subscribe: function() {
        var appId = this.get('appId');
        var topic = this.resourceTopic('Containers', {
            appId: appId
        });
        this.listenTo(this.dataSource, topic, function(res) {
            // Extract container list
            var containers = res.containers;
            
            // Check that this is the expected format
            if ( !(containers instanceof Array) ) {
                throw new Error('The published topic apps.[APPID].containers.list was not in the expected format. Expected: { containers: [] }');
            }
            
            // Extract the correct container
            var updates = _.find(containers, function(ctnr) {
                return ctnr.id === this.get('id');
            }, this);
            
            // If container cannot be found, it should be removed
            if (!updates) {
                this.stopListening(this.dataSource, topic);
                this.operators.unsubscribe();
                this.set('state', 'ENDED');
                this.trigger('remove');
                return;
            }

            // Update the model and trigger 'update' custom event
            this.set(updates);
            this.trigger('update');
        });
        this.dataSource.subscribe(topic);
        
        if (this.operators) {
            // Listen for updates
            this.operators.subscribe();
        }
    },

    kill: function() {
        var appId = this.get('appId') || (this.collection ? this.collection.appId : undefined);
        if (!appId) {
            LOG(3, 'Container:kill requires the presence of an appId.');
            return;
        }
        $.ajax({
            type: 'POST',
            url: this.resourceAction('killContainer', {
                appId: appId,
                containerId: this.get('id')
            }),
            success: function() {
                Notifier.success({
                    title: text('kill_ctnr_sent_title'),
                    text: text('kill_ctnr_sent_text')
                });
            },
            error: function(xhr, textStatus, errorThrown) {
                Notifier.error({
                    title: text('kill_ctnr_fail_title'),
                    text: text('kill_ctnr_fail_text')(errorThrown)
                });
            }
        });
    }
    
});
exports = module.exports = ContainerModel;