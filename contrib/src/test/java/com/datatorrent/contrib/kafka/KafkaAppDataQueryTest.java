/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.kafka;

import org.junit.Test;

public class KafkaAppDataQueryTest
{
  @Test
  public void testGetAppDataURL() throws Exception
  {
    /*
    final String topic = "testTopic";
    final String broker1 = "mybroker:11";
    final String broker2 = "mybroker:22";

    KafkaAppDataQuery appDataQuery = new KafkaAppDataQuery();
    KafkaConsumer consumer = new SimpleKafkaConsumer();
    consumer.setTopic(topic);
    consumer.setBrokerSet(Sets.newHashSet(broker1, broker2));
    appDataQuery.setConsumer(consumer);

    String url1 = "kafka://" + broker1 + "/?brokerSet=" + URLEncoder.encode(broker1 + "," + broker2, "UTF-8");
    String url2 = "kafka://" + broker2 + "/?brokerSet=" + URLEncoder.encode(broker2 + "," + broker1, "UTF-8");

    String genUrl = appDataQuery.getAppDataURL();

    if(url1.equals(genUrl) || url2.equals(genUrl)) {
      return;
    }

    Assert.assertEquals("The url is not correct", url1, genUrl);*/
  }
}
