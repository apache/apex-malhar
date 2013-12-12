package com.datatorrent.contrib.netflow;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

public class KafkaOutputTest
{

  @Test
  public void test(){
    Properties props = new Properties();
    props.setProperty("serializer.class", "kafka.serializer.StringEncoder");
    props.put("metadata.broker.list", "localhost:9092");
    props.setProperty("producer.type", "async");
    props.setProperty("queue.buffering.max.ms", "200");
    props.setProperty("queue.buffering.max.messages", "10");
    props.setProperty("batch.num.messages", "5");
    KafkaOutput<String> k = new KafkaOutput<String>(props);
    Map<Integer,String> m = new HashMap<Integer, String>();
    m.put(new Integer(1), "1");
    m.put(new Integer(2), "2");
    k.process(m.toString());
    k.close();   
  }
}
