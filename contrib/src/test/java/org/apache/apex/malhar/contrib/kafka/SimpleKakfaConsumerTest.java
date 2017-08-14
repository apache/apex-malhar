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
package org.apache.apex.malhar.contrib.kafka;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.util.KryoCloneUtils;

public class SimpleKakfaConsumerTest
{

  @Test
  public void cloneTest() throws Exception
  {
    SimpleKafkaConsumer kc = new SimpleKafkaConsumer();
    int bufferSize = 1000;
    int cacheSize = 100;
    kc.setBufferSize(bufferSize);
    kc.setCacheSize(cacheSize);
    kc.setMetadataRefreshInterval(40000);
    kc.setMetadataRefreshRetryLimit(2);
    kc.setTopic("test_topic");
    kc.setClientId("test_clientid");

    SimpleKafkaConsumer kcClone = KryoCloneUtils.cloneObject(kc);
    Assert.assertEquals("Buffer size is " + bufferSize, bufferSize, kcClone.getBufferSize());
    Assert.assertEquals("Cache size is " + cacheSize, cacheSize, kcClone.getCacheSize());
    Assert.assertEquals("Clint id is same", kc.getClientId(), kcClone.getClientId());
    Assert.assertEquals("Timeout is same", kc.getTimeout(), kcClone.getTimeout());
    Assert.assertEquals("Topic", kc.getTopic(), kcClone.getTopic());
    Assert.assertEquals("Metadata refresh", kc.getMetadataRefreshInterval(), kcClone.getMetadataRefreshInterval());
    Assert.assertEquals("Metadata Retry Limit", kc.getMetadataRefreshRetryLimit(), kcClone.getMetadataRefreshRetryLimit());
  }
}
