/**
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
package org.apache.apex.malhar.kafka;

import java.util.Properties;

import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.MockTime;
import kafka.utils.TestUtils;
import kafka.utils.Time;
import kafka.utils.ZkUtils;

public class EmbeddedKafka extends AbstractEmbeddedKafka
{
  @Override
  public KafkaServer createKafkaServer(Properties prop)
  {
    KafkaConfig config = new KafkaConfig(prop);
    Time mock = new MockTime();
    return TestUtils.createServer(config, mock);
  }

  @Override
  public void createTopic(String topic, ZkUtils zkUtils, int noOfPartitions)
  {
    AdminUtils.createTopic(zkUtils, topic, noOfPartitions, 1, new Properties());
  }
}
