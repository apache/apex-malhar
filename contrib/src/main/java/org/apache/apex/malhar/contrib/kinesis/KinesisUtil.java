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

import java.util.List;
import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.DescribeStreamRequest;
import com.amazonaws.services.kinesis.model.DescribeStreamResult;
import com.amazonaws.services.kinesis.model.GetRecordsRequest;
import com.amazonaws.services.kinesis.model.GetRecordsResult;
import com.amazonaws.services.kinesis.model.GetShardIteratorRequest;
import com.amazonaws.services.kinesis.model.GetShardIteratorResult;
import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.model.Shard;
import com.amazonaws.services.kinesis.model.ShardIteratorType;

/**
 * A util class for Amazon Kinesis. Contains the wrappers for creating client, get the shard list and records from shard.
 *
 * @since 2.0.0
 */
public class KinesisUtil
{

  private transient AmazonKinesisClient client = null;

  private static KinesisUtil instance = null;

  private KinesisUtil()
  {
  }

  public static KinesisUtil getInstance()
  {
    if (instance == null) {
      instance = new KinesisUtil();
    }
    return instance;
  }

  /**
   * Create the AmazonKinesisClient with the given credentials
   * @param accessKey AWS accessKeyId
   * @param secretKey AWS secretAccessKey
   * @throws Exception
   */
  public void createKinesisClient(String accessKey, String secretKey, String endPoint) throws Exception
  {
    if (client == null) {
      try {
        client = new AmazonKinesisClient(new BasicAWSCredentials(accessKey, secretKey));
        if (endPoint != null) {
          client.setEndpoint(endPoint);
        }
      } catch (Exception e) {
        throw new AmazonClientException("Unable to load credentials", e);
      }
    }
  }

  /**
   * Get the available shards from the kinesis
   * @param streamName Name of the stream from where the shards to be accessed
   * @return the list of shards from the given stream
   */
  public List<Shard> getShardList(String streamName)
  {
    assert client != null : "Illegal client";
    DescribeStreamRequest describeRequest = new DescribeStreamRequest();
    describeRequest.setStreamName(streamName);

    DescribeStreamResult describeResponse = client.describeStream(describeRequest);
    return describeResponse.getStreamDescription().getShards();
  }

  /**
   * Get the records from the particular shard
   * @param streamName Name of the stream from where the records to be accessed
   * @param recordsLimit Number of records to return from shard
   * @param shId Shard Id of the shard
   * @param iteratorType Shard iterator type
   * @param seqNo Record sequence number
   * @return the list of records from the given shard
   * @throws AmazonClientException
   */
  public List<Record> getRecords(String streamName, Integer recordsLimit, String shId, ShardIteratorType iteratorType, String seqNo)
    throws AmazonClientException
  {
    assert client != null : "Illegal client";
    try {
      // Create the GetShardIteratorRequest instance and sets streamName, shardId and iteratorType to it
      GetShardIteratorRequest iteratorRequest = new GetShardIteratorRequest();
      iteratorRequest.setStreamName(streamName);
      iteratorRequest.setShardId(shId);
      iteratorRequest.setShardIteratorType(iteratorType);

      // If the iteratorType is AFTER_SEQUENCE_NUMBER, set the sequence No to the iteratorRequest
      if (ShardIteratorType.AFTER_SEQUENCE_NUMBER.equals(iteratorType) ||
          ShardIteratorType.AT_SEQUENCE_NUMBER.equals(iteratorType)) {
        iteratorRequest.setStartingSequenceNumber(seqNo);
      }
      // Get the Response from the getShardIterator service method & get the shardIterator from that response
      GetShardIteratorResult iteratorResponse = client.getShardIterator(iteratorRequest);
      // getShardIterator() specifies the position in the shard
      String iterator = iteratorResponse.getShardIterator();

      // Create the GetRecordsRequest instance and set the recordsLimit and iterator
      GetRecordsRequest getRequest = new GetRecordsRequest();
      getRequest.setLimit(recordsLimit);
      getRequest.setShardIterator(iterator);

      // Get the Response from the getRecords service method and get the data records from that response.
      GetRecordsResult getResponse = client.getRecords(getRequest);
      return getResponse.getRecords();
    } catch (AmazonClientException e) {
      throw new RuntimeException(e);
    }
  }

  public AmazonKinesisClient getClient()
  {
    return client;
  }

  public void setClient(AmazonKinesisClient client)
  {
    this.client = client;
  }
}
