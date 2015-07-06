/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
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
import com.amazonaws.services.kinesis.model.ResourceInUseException;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a base class setup/clean Kinesis testing environment for all the input/output test
 * If it's a multipartition test, this class creates 2 kinesis partitions
 */
public class KinesisOperatorTestBase
{
  public static final String END_TUPLE = "END_TUPLE";
  protected boolean hasMultiPartition = false;
  protected String streamNamePrefix= "StreamName-";
  protected String streamName = null ;
  protected int shardCount = 1;
  protected transient AmazonKinesisClient client = null;
  protected transient AWSCredentialsProvider credentials = null;

  private static final Logger logger = LoggerFactory.getLogger(KinesisOperatorTestBase.class);
  
  private void createClient()
  {
    credentials = new DefaultAWSCredentialsProviderChain();
    client = new AmazonKinesisClient(credentials);
  }

  @Before
  public void beforeTest()
  {
    CreateStreamRequest streamRequest = null;
    createClient();
    
    for( int i=0; i<100; ++i )
    {
      try 
      {
        streamName = streamNamePrefix + i;
        streamRequest = new CreateStreamRequest();
        streamRequest.setStreamName(streamName);
        streamRequest.setShardCount(shardCount);
        client.createStream(streamRequest);
  
        logger.info( "created stream {}.", streamName );
        Thread.sleep(30000);
        
        break;
      }
      catch( ResourceInUseException riue )
      {
        logger.warn( "Resource is in use.", riue.getMessage() );
      }
      catch (Exception e) 
      {
        logger.error( "Got exception.", e );
        throw new RuntimeException(e);
      }
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
