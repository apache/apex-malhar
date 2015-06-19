/**
 * 
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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
 *
 */
package com.datatorrent.contrib.kinesis;

import java.nio.ByteBuffer;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.kinesis.model.Record;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.contrib.util.FieldInfo;
import com.datatorrent.contrib.util.FieldValueGenerator;
import com.datatorrent.contrib.util.POJOTupleGenerateOperator;
import com.datatorrent.contrib.util.TableInfo;
import com.datatorrent.contrib.util.TestPOJO;
import com.datatorrent.contrib.util.TupleGenerator;

public class KinesisPOJOOutputOperatorTest extends KinesisOutputOperatorTest< KinesisPOJOOutputOperator, POJOTupleGenerateOperator >
{ 
  public static class EmployeeTupleGenerateOperator extends POJOTupleGenerateOperator< TestPOJO >
  {
    public EmployeeTupleGenerateOperator()
    {
      super( TestPOJO.class );
      setTupleNum(maxTuple);
    }
  }
  

  @Test
  public void testKinesisOutputOperatorInternal() throws Exception
  {
    KinesisPOJOOutputOperator operator = new KinesisPOJOOutputOperator();
    configureTestingOperator( operator );
    operator.setBatchProcessing(false);
    
    TableInfo tableInfo = new TableInfo();
    tableInfo.setFieldsInfo( TestPOJO.getFieldsInfo() );
    tableInfo.setRowOrIdExpression( TestPOJO.getRowExpression() );
    operator.setTableInfo( tableInfo );

    operator.setup(null);
    
    TupleGenerator generator = new TupleGenerator( TestPOJO.class );
    
    //read tuples
    KinesisTestConsumer listener = createConsumerListener(streamName);
    String iterator = listener.prepareIterator();
    //save the tuples
    for( int i=0; i<maxTuple; ++i )
    {
      if( i%2==0)
        iterator = listener.processNextIterator(iterator);
      
      operator.processTuple( generator.getNextTuple() );
    }
    iterator = listener.processNextIterator(iterator);
  }
  
  @Override
  protected POJOTupleGenerateOperator addGenerateOperator(DAG dag)
  {
    return dag.addOperator("TestPojoGenerator", EmployeeTupleGenerateOperator.class);
  }

  @Override
  protected DefaultOutputPort getOutputPortOfGenerator(POJOTupleGenerateOperator generator)
  {
    return generator.outputPort;
  }

  @Override
  protected KinesisPOJOOutputOperator addTestingOperator(DAG dag)
  {
    KinesisPOJOOutputOperator operator = dag.addOperator("Test-KinesisPojoOutputOperator", KinesisPOJOOutputOperator.class);
    //table info
    {
      TableInfo tableInfo = new TableInfo();
      tableInfo.setFieldsInfo( TestPOJO.getFieldsInfo() );
      tableInfo.setRowOrIdExpression( TestPOJO.getRowExpression() );
      operator.setTableInfo( tableInfo );
    }
    operator.setBatchProcessing(true);
    
    return operator;
  }

  /**
   * add Consumer to process the record
   */
  @Override
  protected KinesisTestConsumer createConsumerListener( String streamName )
  {
    KinesisEmployeeConsumer listener = new KinesisEmployeeConsumer(streamName);
    return listener;
  }
  
  
  public static class KinesisEmployeeConsumer extends KinesisTestConsumer
  {
    private static final Logger logger = LoggerFactory.getLogger( KinesisEmployeeConsumer.class );
    protected FieldValueGenerator<FieldInfo> fieldValueGenerator = FieldValueGenerator.getFieldValueGenerator(TestPOJO.class, TestPOJO.getFieldsInfo() );
    
    public KinesisEmployeeConsumer(String streamNamem )
    {
      super(streamNamem);
    }
    
    @Override
    protected void processRecord( Record record )
    {
      String partitionKey = record.getPartitionKey();
      ByteBuffer data = record.getData();
      logger.info( "partitionKey={} ", partitionKey );
      byte[] dataBytes = new byte[ data.remaining() ];
      data.get( dataBytes, 0, dataBytes.length );
      
      long key = Long.valueOf( partitionKey );
      TestPOJO expected = new TestPOJO( key );
      
      TestPOJO read = (TestPOJO)fieldValueGenerator.deserializeObject( dataBytes );
      
      if( !read.outputFieldsEquals(expected) )
      {
        logger.error( "read is not same as expected. read={}, expected={}", read, expected );
        Assert.assertTrue(false);
      }
      else
      {
        logger.info( "read is same as expected. read={}, expected={}", read, expected );
      }
    }
  }
}
