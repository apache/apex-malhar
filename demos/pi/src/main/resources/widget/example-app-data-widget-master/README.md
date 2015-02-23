Example App Data Widget
=======================
This project is an example of a custom widget that can be used by the DataTorrent App Data Framework. The UI piece of this framework uses [malhar-angular-dashboard](https://github.com/DataTorrent/malhar-angular-dashboard) for its widget and dashboard management, so it would be a good idea to understand the basics of that library before trying to build your own widget.


Widget Spec
------------------
A custom widget must be in the form of a directory with the following requirements:

1. It must have a `package.json` file with values in the following fields:.

    - **main** ( `String[]` or `String` )
    
        Path(s) to the file(s) that 
        actually define the widget. Can include `js` or `css` files. 
        
        _Single File Example:_
        ```JSON
        "main": "build/widget.js"
        ```
        
        _Multiple File Example:_
        ```JSON
        "main": [
            "build/widget.js",
            "build/widget.css"
        ]
        ```
        
    - **schemaType** (`String`)

        The schema type that this widget understands.

    - **schemaVersion** (`String`)
        
        The schema type version that this widget understands.

    - **moduleName** (`String`)
    
        The angular module name that the widget is a part of. This can be an arbitrary module name, but should not conflict with any module names already loaded in the DataTorrent Management Console. The safest way to ensure this is to namespace your widget module, for example: `com.yourCompany.widgets.widgetName`.

    - **serviceName** (`String`)
        
        The actual injectable service that this widget module exposes. As with the module name, it is important to ensure that no other services by the same name exist in the DataTorrent Management Console.

1. It must define a new angular module in one of the files specified by the `main` field in the `package.json`.

    Example:
    ```js
    angular.module('com.yourCompany.ui.widgets.widgetName', [
        'ui.dashboard',
        // other dependencies go here
    ]);
    ```
    
1. It must add the [widget definition](https://github.com/DataTorrent/malhar-angular-dashboard#widget-definition-objects) as a service on the angular module defined in the previous requirement.

    Example:
    ```js
    angular.module('com.yourCompany.ui.widgets.widgetName')
    .factory('widgetName', function() {
        // Widget Definition Object
        // @see: https://github.com/DataTorrent/malhar-angular-dashboard#widget-definition-objects
        return {
            name: 'example widget',
            directive: 'example-widget-directive'
        };
    });
    ```
    

How this example is set up
--------------------------
This example represents a realistic implementation of a custom widget because it sets up a development environment, build tasks, and tests.