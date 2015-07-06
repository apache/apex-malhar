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

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.kinesis.AmazonKinesisClient;
import com.amazonaws.services.kinesis.model.PutRecordRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.common.util.Pair;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
/**
 * Base implementation of Kinesis Output Operator. Convert tuples to records and emits to Kinesis.<br/>
 *
 * Configurations:<br/>
 * {@link #accessKey} : AWS Credentials AccessKeyId <br/>
 * {@link #secretKey} : AWS Credentials SecretAccessKey <br/>
 * streamName : Name of the stream from where the records to be accessed
 *
 * @param <T>
 * @since 2.0.0
 */
public abstract class AbstractKinesisOutputOperator<V, T> implements Operator
{
  private static final Logger logger = LoggerFactory.getLogger( AbstractKinesisOutputOperator.class );
  protected String streamName;
  @NotNull
  private String accessKey;
  @NotNull
  private String secretKey;
  private String endPoint;
  protected static transient AmazonKinesisClient client = null;
  protected int sendCount;
  protected boolean isBatchProcessing = true;

  /**
   * convert the value to record. the value is value of KeyValue pair.
   * @see tupleToKeyValue()
   * @param value
   * @return
   */
  protected abstract byte[] getRecord(V value);
  
  /**
   * convert tuple to pair of key and value. the key will be used as PartitionKey, and the value used as Data
   * @param tuple
   * @return
   */
  protected abstract Pair<String, V> tupleToKeyValue(T tuple);
  
  List<PutRecordsRequestEntry> putRecordsRequestEntryList = new ArrayList<PutRecordsRequestEntry>();
  // Max size of each record: 50KB, Max size of putRecords: 4.5MB
  // So, default capacity would be 4.5MB/50KB = 92
  @Min(2)
  @Max(500)
  protected int batchSize = 92;
  /**
   * Implement Operator Interface.
   */
  @Override
  public void beginWindow(long windowId)
  {
  }

  /**
   * Implement Component Interface.
   */
  @Override
  public void teardown()
  {
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void endWindow()
  {
    if (isBatchProcessing && putRecordsRequestEntryList.size() != 0) {
      try {
        flushRecords();
      } catch (AmazonClientException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * Implement Component Interface.
   *
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    try {
      KinesisUtil.getInstance().createKinesisClient(accessKey, secretKey, endPoint);
    } catch(Exception e)
    {
      throw new RuntimeException("Unable to load Credentials", e);
    }
    this.setClient(KinesisUtil.getInstance().getClient());
    if(isBatchProcessing)
    {
      putRecordsRequestEntryList.clear();
    }
  }

  /**
   * This input port receives tuples that will be written out to Kinesis.
   */
  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      processTuple( tuple );
    }
    
  };

  public void processTuple(T tuple)
  {
    // Send out single data
    try {
      if(isBatchProcessing)
      {
        if(putRecordsRequestEntryList.size() == batchSize)
        {
          flushRecords();
          logger.debug( "flushed {} records.", batchSize );
        }
        addRecord(tuple);

      } else {
        Pair<String, V> keyValue = tupleToKeyValue(tuple);
        PutRecordRequest requestRecord = new PutRecordRequest();
        requestRecord.setStreamName(streamName);
        requestRecord.setPartitionKey(keyValue.first);
        requestRecord.setData(ByteBuffer.wrap(getRecord(keyValue.second)));

        client.putRecord(requestRecord);
        
      }
      sendCount++;
    } catch (AmazonClientException e) {
      throw new RuntimeException(e);
    }
  }
  
  
  private void addRecord(T tuple)
  {
    try {
      Pair<String, V> keyValue = tupleToKeyValue(tuple);
      PutRecordsRequestEntry putRecordsEntry = new PutRecordsRequestEntry();
      putRecordsEntry.setData(ByteBuffer.wrap(getRecord(keyValue.second)));
      putRecordsEntry.setPartitionKey(keyValue.first);
      putRecordsRequestEntryList.add(putRecordsEntry);
    } catch (AmazonClientException e) {
      throw new RuntimeException(e);
    }
  }

  private void flushRecords()
  {
    try {
      PutRecordsRequest putRecordsRequest = new PutRecordsRequest();
      putRecordsRequest.setStreamName(streamName);
      putRecordsRequest.setRecords(putRecordsRequestEntryList);
      client.putRecords(putRecordsRequest);
      putRecordsRequestEntryList.clear();
      logger.debug( "Records flushed." );
    } catch (AmazonClientException e) {
      logger.warn( "PutRecordsRequest exception.", e );
      throw new RuntimeException(e);
    }
  }
  public void setClient(AmazonKinesisClient _client)
  {
    client = _client;
  }

  public String getStreamName()
  {
    return streamName;
  }

  public void setStreamName(String streamName)
  {
    this.streamName = streamName;
  }

  public int getBatchSize()
  {
    return batchSize;
  }

  public void setBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
  }

  public boolean isBatchProcessing()
  {
    return isBatchProcessing;
  }

  public void setBatchProcessing(boolean isBatchProcessing)
  {
    this.isBatchProcessing = isBatchProcessing;
  }

  public String getAccessKey()
  {
    return accessKey;
  }

  public void setAccessKey(String accessKey)
  {
    this.accessKey = accessKey;
  }

  public String getSecretKey()
  {
    return secretKey;
  }

  public void setSecretKey(String secretKey)
  {
    this.secretKey = secretKey;
  }

  public String getEndPoint()
  {
    return endPoint;
  }

  public void setEndPoint(String endPoint)
  {
    this.endPoint = endPoint;
  }
}
