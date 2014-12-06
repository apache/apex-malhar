package com.datatorrent.contrib.kafka;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;

public class SimpleKakfaConsumerTest
{

  @Test
  public void cloneTest()
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

    SimpleKafkaConsumer kcClone = (SimpleKafkaConsumer) kc.cloneConsumer(new HashSet(), new HashMap());
    Assert.assertEquals("Buffer size is " + bufferSize, bufferSize, kcClone.getBufferSize());
    Assert.assertEquals("Cache size is " + cacheSize, cacheSize, kcClone.getCacheSize());
    Assert.assertEquals("Clint id is same", kc.getClientId(), kcClone.getClientId());
    Assert.assertEquals("Timeout is same", kc.getTimeout(), kcClone.getTimeout());
    Assert.assertEquals("Topic", kc.getTopic(), kcClone.getTopic());
    Assert.assertEquals("Metadata refresh", kc.getMetadataRefreshInterval(), kcClone.getMetadataRefreshInterval());
    Assert.assertEquals("Metadata Retry Limit", kc.getMetadataRefreshRetryLimit(), kcClone.getMetadataRefreshRetryLimit());
  }
}
