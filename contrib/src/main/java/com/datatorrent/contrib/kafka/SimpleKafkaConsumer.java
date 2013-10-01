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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.common.ErrorMapping;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.Message;
import kafka.message.MessageAndOffset;

/**
 * Simple kafka consumer used by kafka input operator
 */
public class SimpleKafkaConsumer extends KafkaConsumer
{
  public SimpleKafkaConsumer()
  {
  }
  
  public SimpleKafkaConsumer(String host, int port, int timeout, int bufferSize, String clientId)
  {
    this(host, port, timeout, bufferSize, clientId, -1);
  }
  
  public SimpleKafkaConsumer(String host, int port, int timeout, int bufferSize, String clientId, int partitionId)
  {
    super();
    this.host = host;
    this.port = port;
    this.timeout = timeout;
    this.bufferSize = bufferSize;
    this.clientId = clientId;
    this.partitionId = partitionId;
  }

  private transient SimpleConsumer simpleConsumer;
  @NotNull
  private String host = "localhost";
  
  private int port = 9092;
  
  private int timeout=10000;
  
  //defailt buffer size is 1M
  private int bufferSize = 1024 * 1024;
  
  // default consumer id is "Kafka_Input_Operator_Default_Simple_Client"
  @NotNull
  private String clientId = "Kafka_Input_Operator_Default_Simple_Client";
  
  /**
   * You can setup your particular partitionID you want to consume with *simple
   * kafka consumer*. Use this to maximize the distributed performance 
   * By default it's set to -1 which means #partitionSize anonymous threads will be
   * created to consume tuples from different partition
   */
  @Min(value = -1)
  private int partitionId = -1;
  
  @Override
  public void create()
  {
    super.create();
    simpleConsumer = new SimpleConsumer(host, port, timeout, bufferSize, clientId);
  }

  @Override
  public void start()
  {
    super.start();
    List<Integer> partition = new ArrayList<Integer>();
    if (partitionId == -1) {
      // if partition id is set to -1 , find all the partitions for the specific topic
      List<String> topics = new ArrayList<String>();
      topics.add(topic);
      kafka.javaapi.TopicMetadataRequest req = new kafka.javaapi.TopicMetadataRequest(topics);
      TopicMetadataResponse resp = simpleConsumer.send(req);
      List<TopicMetadata> metaData = (List<TopicMetadata>) resp.topicsMetadata();
      for (TopicMetadata item : metaData) {
        for (PartitionMetadata part : item.partitionsMetadata()) {
          partition.add(part.partitionId());
        }
      }
    } else {
      partition.add(partitionId);
    }
    ExecutorService executor = Executors.newFixedThreadPool(partition.size());
    for (final Integer par : partition) {
      executor.submit(new Runnable() {
        @Override
        public void run()
        {
          long offset = 0;
          while (isAlive) {

            FetchRequest req = new FetchRequestBuilder().clientId(clientId).addFetch(topic, par, offset, bufferSize).build();
            FetchResponse fetchResponse = simpleConsumer.fetch(req);

            if (fetchResponse.hasError() && fetchResponse.errorCode(topic, par)==ErrorMapping.OffsetOutOfRangeCode()) {
              return;
            }

            ByteBufferMessageSet messages = fetchResponse.messageSet(topic, par);
            Iterator<MessageAndOffset> itr = messages.iterator();

            if (itr.hasNext() && isAlive) {
              MessageAndOffset msg = itr.next();
              offset = msg.nextOffset();
              ByteBuffer payload = msg.message().payload();
              byte[] bytes = new byte[payload.limit()];
              payload.get(bytes);
              holdingBuffer.add(new Message(bytes));
            }
          }
        }
      });
      
    }

    
  }

  @Override
  public void stop()
  {
    isAlive = false;
    simpleConsumer.close();
  }
  
  public void setBufferSize(int bufferSize)
  {
    this.bufferSize = bufferSize;
  }
  
  public void setClientId(String clientId)
  {
    this.clientId = clientId;
  }
  
  public void setHost(String host)
  {
    this.host = host;
  }
  
  public void setPort(int port)
  {
    this.port = port;
  }
  
  public void setTimeout(int timeout)
  {
    this.timeout = timeout;
  }
}  // End of SimpleKafkaConsumer