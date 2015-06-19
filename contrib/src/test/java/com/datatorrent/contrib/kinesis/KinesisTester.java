package com.datatorrent.contrib.kinesis;

import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.CreateStreamRequest;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.ResourceInUseException;

public class KinesisTester
{
  public static final int MAX_RECORDS = 3;
  protected String streamNamePrefix= "TestStreamName-";
  protected String streamName = null ;
  protected int shardCount = 2;
  protected transient AmazonKinesisClient client = null;
  protected transient AWSCredentialsProvider credentials = null;


  protected int triedCount = 0;
  private static final Logger logger = LoggerFactory.getLogger(KinesisTester.class);
  
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
  
        logger.info( "create stream done. steamName={}", streamName );
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
  
  @Test
  public void putRecords()
  {
    for( int i=0; i<MAX_RECORDS; ++i )
    {
      putRecord( "key"+i, "value"+i );
    }
  }

  public void putRecord( String key, String value )
  {
    try
    {
      PutRecordRequest requestRecord = new PutRecordRequest();
      requestRecord.setStreamName(streamName);
      requestRecord.setPartitionKey( key );
      requestRecord.setData(ByteBuffer.wrap( value.getBytes() ));
  
      client.putRecord(requestRecord);
      logger.info( "===========Saved a record.================" );
    }
    catch( Exception e )
    {
      logger.error( "Exception. ", e );
    }
    
  }
  
  @After
  public void afterTest()
  {
    client.deleteStream(streamName);

  }
}
