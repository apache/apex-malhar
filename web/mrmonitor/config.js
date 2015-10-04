/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
var config = {};
config.resourceManager = {};

config.port = process.env.PORT || 3000;
config.resourceManager.host = 'localhost';
config.resourceManager.port = '8088';

// client settings (passed to the browser)
config.settings = {};
config.settings.hadoop = {};
config.settings.topic = {};

// WebSocket URL is ws://<gateway hostname>:<gateway port>/pubsub
config.settings.webSocketURL = 'ws://localhost:9090/pubsub';

config.settings.hadoop.version = '2';
config.settings.hadoop.api = 'v1';
config.settings.hadoop.host = config.resourceManager.host;
config.settings.hadoop.resourceManagerPort = config.resourceManager.port;
config.settings.hadoop.historyServerPort = '19888';

config.settings.topic.job = 'contrib.summit.mrDebugger.jobResult';
config.settings.topic.map = 'contrib.summit.mrDebugger.mapResult';
config.settings.topic.reduce = 'contrib.summit.mrDebugger.reduceResult';
config.settings.topic.counters = 'contrib.summit.mrDebugger.counterResult';

module.exports = config
