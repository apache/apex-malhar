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
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import org.junit.After;
import org.junit.Before;

/**
 * This is a base class setup/clean Kinesis testing environment for all the input/output test
 * If it's a multipartition test, this class creates 2 kinesis partitions
 */
public class KinesisOperatorTestBase
{
  public static final String END_TUPLE = "END_TUPLE";
  protected boolean hasMultiPartition = false;
  protected String streamName= "StreamName";
  protected int shardCount = 2;
  protected transient AmazonKinesisClient client = null;

  private void createClient()
  {
    AWSCredentialsProvider credentials = new DefaultAWSCredentialsProviderChain();
    client = new AmazonKinesisClient(credentials);
  }

  @Before
  public void beforeTest()
  {
    createClient();
    CreateStreamRequest streamRequest = new CreateStreamRequest();
    streamRequest.setStreamName(streamName);
    streamRequest.setShardCount(shardCount);
    client.createStream(streamRequest);
    try {
      Thread.sleep(30000);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @After
  public void afterTest()
  {
    client.deleteStream(streamName);

  }

  public void setHasMultiPartition(boolean hasMultiPartition)
  {
    this.hasMultiPartition = hasMultiPartition;
  }
}
