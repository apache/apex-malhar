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
package com.datatorrent.contrib.hbase;

import java.util.*;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.util.FieldInfo.SupportType;
import com.datatorrent.lib.util.TableInfo;
import com.datatorrent.contrib.util.TestPOJO;
import com.datatorrent.contrib.util.TupleGenerator;
import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator.ProcessingMode;

import static com.datatorrent.lib.db.jdbc.JdbcNonTransactionalOutputOperatorTest.APP_ID;
import static com.datatorrent.lib.db.jdbc.JdbcNonTransactionalOutputOperatorTest.OPERATOR_ID;


public class HBasePOJOPutOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(HBasePOJOPutOperatorTest.class);
  private static final int TEST_SIZE = 15000;
  private static final int WINDOW_SIZE = 1500;
  
  private HBasePOJOPutOperator operator;
  
  private final long startWindowId = Calendar.getInstance().getTimeInMillis();
  
  public HBasePOJOPutOperatorTest()
  {
  }

  @Before
  public void prepare() throws Exception
  {
    operator = new HBasePOJOPutOperator();
    setupOperator(operator);

    createOrDeleteTable(operator.getStore(), false );
  }
  
  @After
  public void cleanup() throws Exception
  {
    createOrDeleteTable(operator.getStore(), true );
  }
  
  /**
   * this test case only test if HBasePojoPutOperator can save data to the
   * HBase. it doesn't test connection to the other operators
   */
  @Test
  public void testPutInternal() throws Exception
  {
    long windowId = startWindowId;
    try
    {
      int countInWindow = 0;
      for (int i = 0; i < TEST_SIZE; ++i)
      {
        if( countInWindow == 0 )
          operator.beginWindow(windowId++);
        operator.processTuple(getNextTuple());
        if( ++countInWindow == WINDOW_SIZE )
        {
          operator.endWindow();
          countInWindow = 0;
        }
      }

      Thread.sleep(30000);
      
      readRecordsAndVerify( operator );
      
    }
    catch (Exception e)
    {
      logger.error("testPutInternal() exception.", e);
      Assert.fail(e.getMessage());
    }
  }

  protected void createOrDeleteTable(HBaseStore store, boolean isDelete ) throws Exception
  {
    HBaseAdmin admin = null;
    try
    {
      admin = new HBaseAdmin(store.getConfiguration());
      final String tableName = store.getTableName();
      
      if (!admin.isTableAvailable(tableName) && !isDelete )
      {
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        tableDescriptor.addFamily(new HColumnDescriptor("f0"));
        tableDescriptor.addFamily(new HColumnDescriptor("f1"));

        admin.createTable(tableDescriptor);
      }
      else if( isDelete )
      {
        admin.disableTable(tableName);
        admin.deleteTable( tableName );
      }
    }
    catch (Exception e)
    {
      logger.error("exception", e);
      throw e;
    }
    finally
    {
      if (admin != null)
      {
        try
        {
          admin.close();
        }
        catch (Exception e)
        {
          logger.warn("close admin exception. ", e);
        }
      }
    }
  }

  protected void setupOperator(HBasePOJOPutOperator operator)
  {
    configure(operator);

    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(OperatorContext.PROCESSING_MODE, ProcessingMode.AT_LEAST_ONCE);
    attributeMap.put(OperatorContext.ACTIVATION_WINDOW_ID, -1L);
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);

    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(
        OPERATOR_ID, attributeMap);
    
    operator.setup(context);
  }

  protected void configure(HBasePOJOPutOperator operator)
  {
    TableInfo<HBaseFieldInfo> tableInfo = new TableInfo<HBaseFieldInfo>();
    
    tableInfo.setRowOrIdExpression("row");

    List<HBaseFieldInfo> fieldsInfo = new ArrayList<HBaseFieldInfo>();
    fieldsInfo.add( new HBaseFieldInfo( "name", "name", SupportType.STRING, "f0") );
    fieldsInfo.add( new HBaseFieldInfo( "age", "age", SupportType.INTEGER, "f1") );
    fieldsInfo.add( new HBaseFieldInfo( "address", "address", SupportType.STRING, "f1") );

    tableInfo.setFieldsInfo(fieldsInfo);
    operator.setTableInfo(tableInfo);

    HBaseStore store = new HBaseStore();
    store.setTableName("test");
    store.setZookeeperQuorum("localhost");
    store.setZookeeperClientPort(2181);

    operator.setStore(store);

  }

  private TupleGenerator<TestPOJO> tupleGenerator;

  protected Object getNextTuple()
  {
    if( tupleGenerator == null )
      tupleGenerator = new TupleGenerator<TestPOJO>( TestPOJO.class );
    
    return tupleGenerator.getNextTuple();
  }

  protected void resetTupleGenerator()
  {
    if( tupleGenerator == null )
      tupleGenerator = new TupleGenerator<TestPOJO>( TestPOJO.class );
    else
      tupleGenerator.reset();
  }

  protected void readRecordsAndVerify( HBasePOJOPutOperator operator )
  {
    int[] rowIds = new int[ TEST_SIZE ];
    for( int i=1; i<=TEST_SIZE; ++i )
      rowIds[i-1] = 1;
    try
    {
      HTable table = operator.getStore().getTable();
      Scan scan = new Scan();
      ResultScanner resultScanner = table.getScanner(scan);
      
      int recordCount = 0;
      while( true )
      {
        Result result = resultScanner.next();
        if( result == null )
          break;
        
        int rowId = Integer.valueOf( Bytes.toString( result.getRow() ) );
        Assert.assertTrue( "rowId="+rowId+" aut of range" , ( rowId > 0 && rowId <= TEST_SIZE ) );
        Assert.assertTrue( "the rowId="+rowId+" already processed.", rowIds[rowId-1] == 1 );
        rowIds[rowId-1]=0;
        
        List<Cell> cells = result.listCells();
        
        Map<String, byte[]> map = new HashMap<String,byte[]>();
        for( Cell cell : cells )
        {
          String columnName = Bytes.toString( CellUtil.cloneQualifier(cell) );
          byte[] value = CellUtil.cloneValue(cell);
          map.put(columnName, value);
        }
        TestPOJO read = TestPOJO.from(map);
        read.setRowId(rowId);
        TestPOJO expected = new TestPOJO( rowId );
        
        Assert.assertTrue( String.format( "expected %s, get %s ", expected.toString(), read.toString() ), expected.completeEquals(read) );
        recordCount++;
      }
      
      int missedCount = 0;
      if( recordCount != TEST_SIZE )
      {
        logger.error( "unsaved records: " );
        StringBuilder sb = new StringBuilder();
        
        for( int i=0; i<TEST_SIZE; ++i )
        {
          if( rowIds[i] != 0 )
          {
            sb.append(i+1).append(", ");
            missedCount++;
          }
          if( missedCount>0 && ( missedCount%20 == 0 ) )
          {
            logger.error( sb.toString() );
            sb.delete( 0, sb.length() );
          }
        }
        logger.error( sb.toString() );
        logger.error( "End of unsaved records" );
      }
      Assert.assertTrue( "expected total records = " + TEST_SIZE + ", got " + recordCount + ", missed " + missedCount, recordCount==TEST_SIZE );
    }
    catch( Exception e )
    {
      throw new RuntimeException( "exception", e );
    }
  }

}
