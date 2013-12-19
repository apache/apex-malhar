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
 * Logical Operator Collection
 * 
 * Child class of a regular operator class, but
 * groups info by logical operators
 *
**/
var _ = require('underscore');
var WindowId = require('./WindowId');
var BigInteger = require('jsbn');
var OperatorCollection = require('./OperatorCollection');
var LogicalOperatorModel = require('./LogicalOperatorModel');
var LogicalOperatorCollection = OperatorCollection.extend({
	
	debugName: 'Logical Operators',

	model: LogicalOperatorModel,

	subscribe: function() {
		this.checkForDataSource();
        var topic = this.resourceTopic('Operators', {
            appId: this.appId
        });
        this.listenTo(this.dataSource, topic, function(data) {
            this.set(this.responseTransform(data));
        });
        this.dataSource.subscribe(topic);
    },

	responseTransform: function(res) {
		
		// Group by the logical name
		var grouped = _.groupBy(res.operators, "logicalName");
		
		// reduce each subset to aggregated logical operator object
		return _.map(grouped, LogicalOperatorModel.prototype.reducePhysicalOperators); // end of map
	}

});
exports = module.exports = LogicalOperatorCollection;