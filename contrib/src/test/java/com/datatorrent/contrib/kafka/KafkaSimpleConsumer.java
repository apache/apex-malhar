/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.contrib.kafka;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.Iterator;
import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
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
    consumer = new SimpleConsumer("localhost", 2182, 10000, 1024000);
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
    }
    catch (Exception e) {
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
      FetchRequest fetchRequest = new FetchRequest("topic1", 0, offset, 1000000);

      // get the message set from the consumer and print them out
      ByteBufferMessageSet messages = consumer.fetch(fetchRequest);
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
