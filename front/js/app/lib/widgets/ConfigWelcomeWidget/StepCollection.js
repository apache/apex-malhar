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
var StepModel = require('./StepModel');

var StepCollection = Backbone.Collection.extend({

    model: StepModel,

    /**
     * Sets a step as the active one.
     * 
     * @param {String} id    the id of the step to activate
     */
    setActive: function(id) {
        // Checks to ensure that the step being
        // activated actually exists.
        var step = this.get(id);
        if (!step) {
            throw new Error('Step ' + id + ' was not found in the step collection');
        }

        // Deactivates all other steps.
        this.each(function(s) {
            s.set('active', false);
        });

        // Activate the step and return the model
        // for potential chaining/reuse/etc.
        step.set('active', true);
        return step;
    },

    /**
     * Retrieves the currently active step
     * @return {StepModel} the active model
     */
    getActive: function() {
        return this.find(function(step) {
            return step.get('active');
        });
    }

});
exports = module.exports = StepCollection;