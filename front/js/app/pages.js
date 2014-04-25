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

var text = DT.text;

/**
 * Page View Definitions
 * 
 * This file contains a list of objects that
 * are definitions for every page view of the
 * UI.
 * 
 * The keys on each are the following:
 * @param name         {String}           View name, should be unique from other page names (does not have to match with the view's pageName attribute).
 * @param routes       {Array}            List of one or more route expressions that should point to the page. These get extracted in NavModel.initialize.
 * @param view         {Backbone.View}    The page's backbone view. This should be a subclass of BasePageView.js.
 * @param paramList    {Array}            Specifies the key names for the pageParam object that gets injected into a page. @see PageLoaderView.
 * @param mode         {String}           Specifies the mode that the page resides in ('dev'|'ops')
 * @param breadcrumbs  {Array}            Array of objects representing an item of the breadcrumbs. Each object requires a 'name' field and optionally a 'href' field.
*/
exports = module.exports = [

    /**
     * CONFIGURATION MODE
     */
    {
        name: 'LoginPageView',
        routes: ['login','config/login','signin'],
        view: require('./lib/pages/LoginPageView'),
        paramList: [],
        mode: 'config',
        breadcrumbs: [
            {
                name: text('Log In')
            }
        ]
    },
    {
        'name': 'ConfigurationPageView',
        'routes': ['config', '/config', '/configuration'],
        'view': require('./lib/pages/ConfigPageView'),
        'paramList': [],
        'mode': 'config',
        'breadcrumbs': [
            {
                name: text('configuration')
            }
        ]
    },
    {
        'name': 'InstallWizardPageView',
        'routes': ['welcome', 'config/install-wizard'],
        'view': require('./lib/pages/InstallWizardPageView'),
        'paramList': [],
        'mode': 'config',
        'breadcrumbs': [
            {
                name: text('configuration'),
                href: '#config'
            },
            {
                name: text('Installation Wizard')
            }
        ]
    },
    {
        name: 'DiagnosticsPageView',
        routes: ['config/diagnostics'],
        view: require('./lib/pages/DiagnosticsPageView'),
        paramList: [],
        mode: 'config',
        breadcrumbs: [
            {
                name: text('configuration'),
                href: '#config'
            },
            {
                name: text('System Diagnostics')
            }
        ]
    },

    

    {
        name: 'LicensePageView',
        routes: ['config/license'],
        view: require('./lib/pages/LicensePageView'),
        mode: 'config',
        breadcrumbs: [
            {
                name: text('configuration'),
                href: '#config'
            },
            {
                name: text('License Information')
            }
        ]
    },

    // {
    //     name: 'ConfigPropertiesPageView',
    //     routes: ['config/properties'],
    //     view: require('./lib/pages/ConfigPropertiesPageView'),
    //     mode: 'config',
    //     breadcrumbs: [
    //         {
    //             name: text('configuration'),
    //             href: '#config'
    //         },
    //         {
    //             name: text('Properties')
    //         }
    //     ]
    // },

    /**
     * OPERATIONS MODE
     * 
    */
    
    /** Operations View */
    {
        'name': 'OpsHomePageView',
        'routes': ['', '/', 'ops', 'ops/apps'],
        'view': require('./lib/pages/OpsHomePageView'),
        'paramList': [],
        'mode': 'ops',
        'breadcrumbs': [
            {
                name: text('ops_main_breadcrumb')
            }
        ]
    },
    
    /** App Instance View */
    {
        'name': 'AppInstancePageView',
        'routes': ['ops/apps/:appId'],
        'view': require('./lib/pages/AppInstancePageView'),
        'paramList': ['appId'],
        'mode': 'ops',
        'breadcrumbs': [
            // HCURL
            { 
                name: text('ops_main_breadcrumb'), 
                href: '#ops' 
            },
            {
                name: function(page, appId) {
                    return appId;
                }
            }
        ]
    },
    
    /** Physical Operator View */
    {
        'name': 'PhysOpPageView',
        'routes': ['ops/apps/:appId/operators/:operatorId'],
        'view': require('./lib/pages/PhysOpPageView'),
        'paramList': ['appId', 'operatorId'],
        'mode': 'ops',
        'breadcrumbs': [
            // HCURL
            { name: text('ops_main_breadcrumb'), href: '#ops' },
            { name: function(page, appId, operatorId) {
                return appId;
            },href: function(page, appId, operatorId) {
                // HCURL
                return '#ops/apps/'+appId;
            }},
            { name: 'physical operators' },
            { name: function(page, appId, operatorId) {
                return operatorId;
            } }
        ]
    },

    /** Logical Operator View */
    {
        'name': 'LogicalOpPageView',
        'routes': ['ops/apps/:appId/logicalOperators/:logicalName'],
        'view': require('./lib/pages/LogicalOpPageView'),
        'paramList': ['appId', 'logicalName'],
        'mode': 'ops',
        'breadcrumbs': [
            { name: text('ops_main_breadcrumb'), href: '#ops' },
            { name: function(page, appId, operatorId) {
                return appId;
            },href: function(page, appId, operatorId) {
                // HCURL
                return '#ops/apps/'+appId;
            }},
            { 
                name: 'logical operators'
            },
            { 
                name: function(page, appId, logicalName) {
                    return logicalName;
                }
            }
        ]
    },
    
    /** Physical Port View */
    {
        'name': 'PortPageView',
        'routes': ['ops/apps/:appId/operators/:operatorId/ports/:portName'],
        'view': require('./lib/pages/PortPageView'),
        'paramList': ['appId', 'operatorId', 'portName'],
        'mode': 'ops',
        'breadcrumbs': [
            // HCURL
            { name: text('ops_main_breadcrumb'), href: '#ops' },
            { name: function(page, appId, operatorId) {
                return appId;
            },href: function(page, appId, operatorId) {
                // HCURL
                return '#ops/apps/'+appId;
            }},
            { name: function(page, appId, operatorId) {
                return 'operator_'+operatorId;
            },href: function(page, appId, operatorId) {
                // HCURL
                return '#ops/apps/'+appId+'/operators/'+operatorId;
            }},
            { name: function(page, appId, operatorId, portname) {
                return 'port_'+portname;
            }}
        ]
    },

    /** Stream View */
    {
        'name': 'StreamPageView',
        'routes': ['ops/apps/:appId/streams/:streamName'],
        'view': require('./lib/pages/StreamPageView'),
        'paramList': ['appId', 'streamName'],
        'mode': 'ops',
        'breadcrumbs': [
            {
                name: text('ops_main_breadcrumb'),
                // HCURL
                href: '#ops'
            },
            
            {
                name: function(page, appId, streamName) {
                    return appId;
                },
                href: function(page, appId, streamName) {
                    // HCURL
                    return '#ops/apps/' + appId;
                }
            },
            
            {
                name: function(page, appId, streamName) {
                    return 'stream ' + streamName;
                }
            }
        ]
    },
    
    /** Container View */
    {
        'name': 'ContainerPageView',
        'routes': ['ops/apps/:appId/containers/:containerid'],
        'view': require('./lib/pages/ContainerPageView'),
        'paramList': ['appId','containerId'],
        'mode': 'ops',
        'breadcrumbs': [
            {
                name: text('ops_main_breadcrumb'),
                // HCURL
                href: '#ops'
            },
            
            {
                name: function(page, appId, ctnrid) {
                    return appId;
                },
                href: function(page, appId, ctnrid) {
                    // HCURL
                    return '#ops/apps/' + appId;
                }
            },
            
            {
                name: function(page, appId, containerId) {
                    return containerId;
                }
            }
        ]
    },
    
    /** Recording View */
    {
        'name': 'RecordingPageView',
        'routes': ['ops/apps/:appId/operators/:operatorId/recordings/:startTime'],
        'view': require('./lib/pages/RecordingPageView'),
        'paramList': ['appId', 'operatorId', 'startTime'],
        'mode': 'ops',
        'breadcrumbs': [
            // HCURL
            { name: text('ops_main_breadcrumb'), href: '#ops' },
            { name: function(page, appId, operatorId) {
                return appId;
            },href: function(page, appId, operatorId) {
                // HCURL
                return '#ops/apps/'+appId;
            }},
            { name: function(page, appId, operatorId) {
                return 'operator_'+operatorId;
            },href: function(page, appId, operatorId) {
                // HCURL
                return '#ops/apps/'+appId+'/operators/'+operatorId;
            }},
            { name: function(page, appId, operatorId, starttime) {
                return 'recording_'+starttime;
            }}
        ]
    },
    
    /** Instance DAG View */
    {
        'name': 'DagPageView',
        'routes': ['ops/apps/:appId/dag'],
        'view': require('./lib/pages/DagPageView'),
        'paramList': ['appId'],
        'mode': 'ops',
        'breadcrumbs': [
            // HCURL
            {
                name: text('ops_main_breadcrumb'),
                href: '#ops'
            },
            {
                name: function(page, appid) {
                    return appid;
                }
            }
        ]
    },
    
    /** Add Alert View */
    {
        'name': 'AddAlertPageView',
        'routes': [
            'ops/apps/:appId/add_alert',
            'ops/apps/:appId/add_alert/:operatorName',
            'ops/apps/:appId/add_alert/:operatorName/:portName'
        ],
        'view': require('./lib/pages/AddAlertPageView'),
        'paramList': ['appId', 'operatorName', 'portName'],
        'mode': 'ops',
        'breadcrumbs': [
            // HCURL
            {
                name: text('ops_main_breadcrumb'),
                href: '#ops'
            },
            {
                name: function(page, appId) {
                    return appId;
                },
                href: function(page, appId, ctnrid) {
                    // HCURL
                    return '#ops/apps/' + appId;
                }
            },
            {
                name: 'add an alert'
            }
        ]
    },
    
    /**
     * DEVELOPMENT MODE
     * 
    */
    
    /** Development View */
    // {
    //     'name': 'DevHomePageView',
    //     'routes': ['dev'],
    //     'view': require('./lib/pages/DevHomePageView'),
    //     'paramList': [],
    //     'mode': 'dev',
    //     'breadcrumbs': [
    //         {
    //             name: 'development'
    //         }
    //     ]
    // },
    
    /** Jar File Page */
    // {
    //     'name': 'JarFilePageView',
    //     'routes': ['dev/jars/:fileName'],
    //     'view': require('./lib/pages/JarFilePageView'),
    //     'paramList': ['fileName'],
    //     'mode': 'dev',
    //     'breadcrumbs': [
    //         {
    //             name: 'development',
    //             href: '#dev'
    //         },
    //         {
    //             name: 'uploaded jars',
    //             href: '#dev'
    //         },
    //         {
    //             name: function(page, fileName) {
    //                 return fileName;
    //             }
    //         }
    //     ]
    // },
    
    /** Jar Application Page  (an application within a jar) */
    // {
    //     'name': 'JarAppPageView',
    //     'routes': ['dev/jars/:fileName/:appName'],
    //     'view': require('./lib/pages/JarAppPageView'),
    //     'paramList': ['fileName', 'appName'],
    //     'mode': 'dev', 
    //     'breadcrumbs': [
    //         {
    //             name: 'development',
    //             href: '#dev'
    //         },
    //         {
    //             name: 'uploaded jars',
    //             href: '#dev'
    //         },
    //         {
    //             name: function(page, fileName) {
    //                 return fileName;
    //             },
    //             href: function(page, fileName) {
    //                 return '#dev/jars/' + fileName;
    //             }
    //         },
    //         {
    //             name: function(page, fileName, appName) {
    //                 return appName;
    //             }
    //         }
    //     ]
        
    // },
    
    /** Add Alert Template View 
    {
        'name': 'AddAlertTemplatePageView',
        'routes': ['dev/alerts/create_template'],
        'view': require('./lib/pages/AddAlertTemplatePageView'),
        'paramList': [],
        'mode': 'dev',
        'breadcrumbs': [
            // HCURL
            {
                name: 'development',
                href: '#dev'
            },
            {
                name: 'add an alert template'
            }
        ]
    },
    */
    
    /** Error Page */
    /* Keep this at the end of this list so it acts as a catch-all for unspecified URLs */
    {
        'name': 'ErrorPageView',
        'routes': ['*path'],
        'view': require('./lib/pages/ErrorPageView'),
        'paramList': [],
        'mode': ''
    }
    
];