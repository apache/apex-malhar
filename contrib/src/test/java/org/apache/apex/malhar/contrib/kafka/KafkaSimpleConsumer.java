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

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

/**
 *
 */
public class KafkaSimpleConsumer implements Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(KafkaSimpleConsumer.class);
  private SimpleConsumer consumer;
  private Charset charset = Charset.forName("UTF-8");
  private CharsetDecoder decoder = charset.newDecoder();
  private int receiveCount = 0;
  private boolean isAlive = true;

  public KafkaSimpleConsumer()
  {
    // create a consumer to connect to the kafka kserver running on localhost, port 2182, socket timeout of 10 secs, socket receive buffer of ~1MB
    consumer =  new SimpleConsumer("localhost", 2182, 10000, 1024000, "default_client");
  }

  public int getReceiveCount()
  {
    return receiveCount;
  }

  public void setReceiveCount(int receiveCount)
  {
    this.receiveCount = receiveCount;
  }

  public void setIsAlive(boolean isAlive)
  {
    this.isAlive = isAlive;
  }

  public String byteBufferToString(ByteBuffer buffer)
  {
    String data = "";
    try {
      int old_position = buffer.position();
      data = decoder.decode(buffer).toString();
      // reset buffer's position to its original so it is not altered:
      buffer.position(old_position);
    } catch (Exception e) {
      return data;
    }
    return data;
  }

  @Override
  public void run()
  {
    long offset = 0;
    while (isAlive) {
      // create a fetch request for topic “topic1”, partition 0, current offset, and fetch size of 1MB
      FetchRequest fetchRequest = new FetchRequestBuilder().clientId("default_client").addFetch("topic1", 1, offset, 1000000).build();

//      FetchRequest fetchRequest = new FetchRequest("topic1", 0, offset, 1000000);

      // get the message set from the consumer and print them out
      ByteBufferMessageSet messages = consumer.fetch(fetchRequest).messageSet("topic1", 1);
      Iterator<MessageAndOffset> itr = messages.iterator();

      while (itr.hasNext() && isAlive) {
        MessageAndOffset msg = itr.next();
        // advance the offset after consuming each message
        offset = msg.offset();
        logger.debug("consumed: {} offset: {}", byteBufferToString(msg.message().payload()).toString(), offset);
        receiveCount++;
      }
    }
  }

  public void close()
  {
    consumer.close();
  }
}
