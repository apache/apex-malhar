'use strict';

describe('Directive: d3Bars', function () {

  // load the directive's module
  beforeEach(module('mrappApp'));

  var element,
    scope;

  beforeEach(inject(function ($rootScope) {
    scope = $rootScope.$new();
  }));

  it('should make hidden element visible', inject(function ($compile) {
    element = angular.element('<d3-bars></d3-bars>');
    element = $compile(element)(scope);
    expect(element.text()).toBe('this is the d3Bars directive');
  }));
});
