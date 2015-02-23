'use strict';

angular.module('com.datatorrent.ui.appdata.widgets.exampleWidget', [
  'ui.dashboard'
])

/**
  * @ngdoc service
  * @name com.datatorrent.ui.appdata.widgets.exampleWidget
  * @description An example custom widget definition for app data.
  * @requires ui.dashboard
**/
.factory('exampleWidget', function(exampleWidgetDataModel) {

  function exampleWidgetDefinition() {
    this.name = 'example widget';
    this.title = 'An example widget';
    this.directive = 'example-widget-directive';
    this.dataModelType = exampleWidgetDataModel;
    this.dataAttrName = 'data';
  }
  return exampleWidgetDefinition;

})

/**
  * @ngdoc service
  * @name com.datatorrent.ui.appdata.widgets.exampleWidget.exampleWidgetDataModel
**/
.factory('exampleWidgetDataModel', function(WidgetDataModel) {
  function exampleWidgetDataModel() {}
  exampleWidgetDataModel.prototype = Object.create(WidgetDataModel.prototype);
  exampleWidgetDataModel.prototype.init = function() {
    // get schema
    // subscribe to topic, set up query functionality etc

    // this.dataModelOptions.appDataSource should have an
    // interface with query, subscribe, unsubscribe, etc.
  };
  exampleWidgetDataModel.prototype.destroy = function() {
    // unsubscribe to topics, etc.
  };
  return exampleWidgetDataModel;
})

/**
  * @ngdoc directive
  * @name module.directive:exampleWidgetDirective
  * @restrict A
  * @description The inner directive of this widget.
**/
.directive('exampleWidgetDirective', function() {

  return {
    template: '<pre>{{ data | json }}</pre>',
    scope: {
      data: '='
    }
  };

});