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

import java.util.concurrent.ArrayBlockingQueue;

import javax.validation.constraints.NotNull;

import kafka.message.Message;


/**
 * Base Kafka Consumer class used by kafka input operator
 */
public abstract class KafkaConsumer
{
  
  public KafkaConsumer()
  {
  }
  
  public KafkaConsumer(String topic)
  {
    super();
    this.topic = topic;
  }

  private int consumerBuffer = 1024*1024;
  
  protected transient boolean isAlive = false;
  
  protected transient ArrayBlockingQueue<Message> holdingBuffer;
  
  @NotNull
  protected String topic;

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

  public Message pollMessage()
  {
    return holdingBuffer.poll();
  }

  public int messageSize()
  {
    return holdingBuffer.size();
  }

}
