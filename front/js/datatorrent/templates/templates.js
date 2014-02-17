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
var kt = require('knights-templar');

exports = module.exports = {
    
    'app_instance_link': kt.make(__dirname+'/app_instance_link.html','_'),
    
    'phys_op_link': kt.make(__dirname+'/phys_op_link.html','_'),

    'logical_op_link': kt.make(__dirname+'/logical_op_link.html', '_'),
    
    'container_link': kt.make(__dirname+'/container_link.html','_'),
    
    'port_name_link': kt.make(__dirname+'/port_name_link.html','_'),
    
    'recording_name_link': kt.make(__dirname+'/recording_name_link.html','_'),
    
    'stream_name_link': kt.make(__dirname+'/stream_name_link.html','_'),
    
    'jar_view_link': kt.make(__dirname+'/jar_view_link.html', '_'),
    
    'jar_app_view_link': kt.make(__dirname + '/jar_app_view_link.html', '_'),

    'licensed_mem_bar': kt.make(__dirname + '/licensed_mem_bar.html', '_'),

    'ctnr_log_btngrp': kt.make(__dirname + '/ctnr_log_btngrp.html')
    
};