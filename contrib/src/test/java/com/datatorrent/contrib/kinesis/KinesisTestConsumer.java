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
package com.datatorrent.contrib.kinesis;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;

/**
 * A kinesis consumer for testing
 */
public class KinesisTestConsumer implements Runnable
{
  private static final Logger logger = LoggerFactory.getLogger(KinesisConsumer.class);
  protected String streamName;
  protected transient AmazonKinesisClient client = null;

  //private final transient ConsumerConnector consumer;
  protected static final int BUFFER_SIZE_DEFAULT = 1024;
  // Config parameters that user can set.
  private final int bufferSize = BUFFER_SIZE_DEFAULT;
  public transient ArrayBlockingQueue<Record> holdingBuffer = new ArrayBlockingQueue<Record>(bufferSize);

  private boolean isAlive = true;
  private int receiveCount = 0;
  // A latch object to notify the waiting thread that it's done consuming the message
  private CountDownLatch latch;
  private void createClient()
  {
    AWSCredentialsProvider credentials = new DefaultAWSCredentialsProviderChain();
    client = new AmazonKinesisClient(credentials);
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

  public KinesisTestConsumer(String topic)
  {
    createClient();
    this.streamName = topic;
  }

  public String getData(Record rc)
  {
    ByteBuffer buffer = rc.getData();
    byte[] bytes = new byte[buffer.remaining()];
    buffer.get(bytes);
    return new String(bytes);
  }

  @Override
  public void run()
  {
    DescribeStreamRequest describeRequest = new DescribeStreamRequest();
    describeRequest.setStreamName(streamName);

    DescribeStreamResult describeResponse = client.describeStream(describeRequest);
    final List<Shard> shards = describeResponse.getStreamDescription().getShards();
    logger.debug("Inside consumer::run receiveCount= {}", receiveCount);
    while (isAlive ) {
      Shard shId = shards.get(0);
      GetShardIteratorRequest iteratorRequest = new GetShardIteratorRequest();
      iteratorRequest.setStreamName(streamName);
      iteratorRequest.setShardId(shId.getShardId());

      iteratorRequest.setShardIteratorType("TRIM_HORIZON");
      GetShardIteratorResult iteratorResponse = client.getShardIterator(iteratorRequest);
      String iterator = iteratorResponse.getShardIterator();

      GetRecordsRequest getRequest = new GetRecordsRequest();
      getRequest.setLimit(1000);
      getRequest.setShardIterator(iterator);
      //call "get" operation and get everything in this shard range
      GetRecordsResult getResponse = client.getRecords(getRequest);
      //get reference to next iterator for this shard
      //retrieve records
      List<Record> records = getResponse.getRecords();
      if (records == null || records.isEmpty()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
      } else {
        for (Record rc : records) {
          if (latch != null) {
            latch.countDown();
          }
          if(getData(rc).equals(KinesisOperatorTestBase.END_TUPLE))
            break;
          holdingBuffer.add(rc);
          receiveCount++;
          logger.debug("Consuming {}, receiveCount= {}", getData(rc), receiveCount);
        }
      }
    }
    logger.debug("DONE consuming");
  }

  public void close()
  {
    isAlive = false;
    holdingBuffer.clear();
  }

  public void setLatch(CountDownLatch latch)
  {
    this.latch = latch;
  }
}