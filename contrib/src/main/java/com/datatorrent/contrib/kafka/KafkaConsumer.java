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
package com.datatorrent.contrib.kafka;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import javax.validation.constraints.NotNull;
import kafka.message.Message;


/**
 * Base Kafka Consumer class used by kafka input operator
 */
public abstract class KafkaConsumer
{
  protected final static String HIGHLEVEL_CONSUMER_ID_SUFFIX = "_stream_";
  
  protected final static String SIMPLE_CONSUMER_ID_SUFFIX = "_partition_";
  
  public KafkaConsumer()
  {
    Set<String> brokerSet = new HashSet<String>();
    brokerSet.add("localhost:9092");
  }
  
  public KafkaConsumer(String topic)
  {
    this();
    this.topic = topic;
  }
  
  public KafkaConsumer(Set<String> brokerSet, String topic)
  {
    this.topic = topic;
    this.brokerSet = brokerSet;
  }

  private int consumerBuffer = 1024 * 1024;
  
  protected transient boolean isAlive = false;
  
  protected transient ArrayBlockingQueue<Message> holdingBuffer;
  
  /**
   * The topic that this consumer consumes
   */
  @NotNull
  protected String topic = "default_topic";
  
  /**
   * A broker list to retrieve the metadata for the consumer
   * This property could be null
   * But it's mandatory for dynamic partition and fail-over 
   */
  @NotNull
  protected Set<String> brokerSet;

  /**
   * This method is called in setup method of the operator
   */
  public void create(){
    holdingBuffer = new ArrayBlockingQueue<Message>(consumerBuffer);
  };

  /**
   * This method is called in the activate method of the operator
   */
  public void start(){
    isAlive = true;
  };

  /**
   * The method is called in the deactivate method of the operator
   */
  public void stop(){
    isAlive = false;
    holdingBuffer.clear();
  };
  
  /**
   * This method is called in teardown method of the operator
   */
  public void teardown()
  {
    holdingBuffer.clear();
  }
  
  public boolean isAlive()
  {
    return isAlive;
  }
  
  public void setAlive(boolean isAlive)
  {
    this.isAlive = isAlive;
  }

  public void setTopic(String topic)
  {
    this.topic = topic;
  }
  
  public String getTopic()
  {
    return topic;
  }

  public Message pollMessage()
  {
    return holdingBuffer.poll();
  }

  public int messageSize()
  {
    return holdingBuffer.size();
  }
  
  public void setBrokerSet(Set<String> brokerSet)
  {
    this.brokerSet = brokerSet;
  }
  
  public Set<String> getBrokerSet()
  {
    return brokerSet;
  }

  protected abstract KafkaConsumer cloneConsumer(int partitionId);

  protected abstract void commitOffset();
  

}
