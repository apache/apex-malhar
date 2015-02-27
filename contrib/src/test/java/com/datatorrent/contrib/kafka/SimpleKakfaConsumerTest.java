package com.datatorrent.contrib.kafka;

import com.datatorrent.lib.util.TestUtils;
import com.esotericsoftware.kryo.Kryo;
import org.junit.Assert;
import org.junit.Test;

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

    SimpleKafkaConsumer kcClone = TestUtils.clone(new Kryo(), kc);
    Assert.assertEquals("Buffer size is " + bufferSize, bufferSize, kcClone.getBufferSize());
    Assert.assertEquals("Cache size is " + cacheSize, cacheSize, kcClone.getCacheSize());
    Assert.assertEquals("Clint id is same", kc.getClientId(), kcClone.getClientId());
    Assert.assertEquals("Timeout is same", kc.getTimeout(), kcClone.getTimeout());
    Assert.assertEquals("Topic", kc.getTopic(), kcClone.getTopic());
    Assert.assertEquals("Metadata refresh", kc.getMetadataRefreshInterval(), kcClone.getMetadataRefreshInterval());
    Assert.assertEquals("Metadata Retry Limit", kc.getMetadataRefreshRetryLimit(), kcClone.getMetadataRefreshRetryLimit());
  }
}
