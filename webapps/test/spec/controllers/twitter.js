'use strict';

describe('Controller: TwitterCtrl', function () {

  // load the controller's module
  beforeEach(module('mrappApp'));

  var TwitterCtrl,
    scope;

  // Initialize the controller and a mock scope
  beforeEach(inject(function ($controller, $rootScope) {
    scope = $rootScope.$new();
    TwitterCtrl = $controller('TwitterCtrl', {
      $scope: scope
    });
  }));

  it('should attach a list of awesomeThings to the scope', function () {
    expect(scope.awesomeThings.length).toBe(3);
  });
});
