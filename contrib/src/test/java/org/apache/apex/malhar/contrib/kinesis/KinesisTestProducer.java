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
package org.apache.apex.malhar.contrib.kinesis;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;

/**
 * A kinesis producer for testing
 */
public class KinesisTestProducer implements Runnable
{
  protected String streamName;
  protected transient AmazonKinesisClient client = null;

  private int sendCount = 20;
  private int batchSize = 92;
  List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<PutRecordsRequestEntry>();
  private boolean hasPartition = false;
  private List<String> records;

  public void setRecords(List<String> records)
  {
    this.records = records;
  }

  private void createClient()
  {
    AWSCredentialsProvider credentials = new DefaultAWSCredentialsProviderChain();
    client = new AmazonKinesisClient(credentials);
  }

  public KinesisTestProducer(String topic)
  {
    this(topic, false);
  }

  public KinesisTestProducer(String topic, boolean hasPartition)
  {
    this.streamName = topic;
    this.hasPartition = hasPartition;
    createClient();
  }

  private void generateRecords()
  {
    // Create dummy message
    int recordNo = 1;
    while (recordNo <= sendCount) {
      String dataStr = "Record_" + recordNo;
      PutRecordsRequestEntry putRecordsEntry = new PutRecordsRequestEntry();
      putRecordsEntry.setData(ByteBuffer.wrap(dataStr.getBytes()));
      putRecordsEntry.setPartitionKey(dataStr);
      putRecordsRequestEntryList.add(putRecordsEntry);
      if ( (putRecordsRequestEntryList.size() == batchSize) || (recordNo == sendCount )) {
        PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
        putRecordsRequest.setStreamName(streamName);
        putRecordsRequest.setRecords(putRecordsRequestEntryList);
        client.putRecords(putRecordsRequest);
        putRecordsRequestEntryList.clear();
      }
      recordNo++;
    }
  }

  @Override
  public void run()
  {
    if (records == null) {
      generateRecords();
    } else {
      for (String msg : records) {
        PutRecordRequest putRecordRequest = new PutRecordRequest();
        putRecordRequest.setStreamName(streamName);
        putRecordRequest.setData(ByteBuffer.wrap(msg.getBytes()));
        putRecordRequest.setPartitionKey(msg);
        client.putRecord(putRecordRequest);
      }
    }
  }

  public int getBatchSize()
  {
    return batchSize;
  }

  public void setBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
  }

  public int getSendCount()
  {
    return sendCount;
  }

  public void setSendCount(int sendCount)
  {
    this.sendCount = sendCount;
  }
} // End of KinesisTestProducer
