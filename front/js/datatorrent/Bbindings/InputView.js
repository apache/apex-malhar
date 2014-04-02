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
 * Bind a backbone model attribute to an input
 * 
*/

var _ = require('underscore'), Backbone = require('backbone');
var InputView = Backbone.View.extend({
    
    tagName: 'input',
    
    default_events: ['change'],
    
    initialize:function(options) {
        // Fill options with defaults
        _.defaults(options, {
            updateEvents: this.default_events,
            clearErrorOnFocus: false,
            errorClass: 'validation-error',
            autoRevert: false,
            setAnyway: false,
            listenToModel: true,
            delayUpdate: false,
            classElement: false,
            // supply a function here that returns an element 
            // and the element will be filled with validation
            // error. If string, will use this.$el.parent().find(errorEl)
            // to find the element.
            errorEl: false
        });

        if (options.attr === undefined) {
            throw new Error('bbind.text requires an "attr" property in its options, which points to an attribute on the model');
        }
        
        // Store the attribute to bind to
        this.attr = options.attr;
        
        // Listen to changes on this attribute
        if (options.listenToModel) {
            this.listenTo(this.model, 'change:'+this.attr, this.render);
        }
        
        // Save options to this
        this.options = options;
        
    },
    
    events: function() {
        var eventHash = {};
        for (var i = this.options.updateEvents.length - 1; i >= 0; i--){
            eventHash[this.options.updateEvents[i]] = 'updateValue';
        }

        if (this.options.clearErrorOnFocus) {
            eventHash['focus'] = 'clearError';
        }

        return eventHash;
    },

    clearError: function () {
        var $el = this.getClassElement();
        var $err = this.getErrorElement();

        $el.removeClass(this.options.errorClass);
        if ($err) {
            $err.html('');
        }
    },

    getClassElement: function() {
        return this.options.classElement ? this.options.classElement(this.$el) : this.$el ;
    },

    getErrorElement: function() {
        if (this.options.errorEl !== false) {
            switch (typeof this.options.errorEl) {
                case 'function':
                    return this.options.errorEl(this.$el);

                case 'string':
                    return this.$el.parent().find(this.options.errorEl);
            }
        }
        return false;
    },
    
    setValue: function(updates) {
        
        // get the element to add/remove error class to
        var $el = this.getClassElement();

        // get element with error message if this.model.errorClass is set
        var $err = this.getErrorElement();
        
        // try setting
        var response = this.model.set( updates, { validate: true } );
        
        // check the response
        if (!response) {
            // get the error condition
            var validationError = this.model.validationError;
            
            // if the error is an object, this assumes that it
            // is a hash where the keys are invalid attributes and 
            // values are the error messages for those attributes.
            if (_.isObject(validationError)) {
                
                // check for an error on this attribute
                if (validationError.hasOwnProperty( this.attr )) {
                    validationError = validationError[this.attr];
                }
                // no error, re-set the attribute without validation
                else {
                    this.model.set( updates );
                    validationError = false;
                }
                
            }
            
            // still an error: add the class, trigger an error, and re-render
            if (validationError) {
                $el.addClass(this.options.errorClass);
                this.trigger('error', validationError);
                if ($err) {
                    $err.text(validationError);
                }
                if (this.options.autoRevert) {
                    this.render();
                }
                if (this.options.setAnyway) {
                    this.model.set( updates );
                }
                return;
            }
        }


        // If it made it to here, it is valid and has been set.
        // remove the error class and return;
        this.clearError();

        return;
        
    }
    
});
exports = module.exports = InputView;