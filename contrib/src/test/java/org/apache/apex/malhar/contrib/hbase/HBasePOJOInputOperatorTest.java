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
package org.apache.apex.malhar.contrib.hbase;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.apex.malhar.contrib.util.TestPOJO;
import org.apache.apex.malhar.contrib.util.TupleCacheOutputOperator;
import org.apache.apex.malhar.contrib.util.TupleGenerateCacheOperator;
import org.apache.apex.malhar.lib.util.FieldInfo.SupportType;
import org.apache.apex.malhar.lib.util.TableInfo;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;

import static org.apache.apex.malhar.lib.db.jdbc.JdbcNonTransactionalOutputOperatorTest.OPERATOR_ID;
import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

public class HBasePOJOInputOperatorTest
{
  public static enum OPERATOR
  {
    GENERATOR,
    HBASEOUTPUT,
    HBASEINPUT,
    OUTPUT
  }

  public static class MyGenerator extends TupleGenerateCacheOperator<TestPOJO>
  {
    public MyGenerator()
    {
      this.setTupleType( TestPOJO.class );
    }
  }

  public static class TestHBasePOJOInputOperator extends HBasePOJOInputOperator
  {
    @Override
    public void setup(OperatorContext context)
    {
      try {
        // Added to let the output operator insert data into hbase table before input operator can read it
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      super.setup(context);
    }
  }

  private static final Logger logger = LoggerFactory.getLogger( HBasePOJOInputOperatorTest.class );
  private final int TUPLE_NUM = 1000;
  private final long RUN_DURATION = 30000; // time in ms
  private HBaseStore store;
  private HBasePOJOPutOperator hbaseOutputOperator;
  private TestHBasePOJOInputOperator hbaseInputOperator;

  @Before
  public void prepare() throws Exception
  {
    hbaseInputOperator = new TestHBasePOJOInputOperator();
    hbaseOutputOperator = new HBasePOJOPutOperator();
    setupOperators();
    HBaseUtil.createTable( store.getConfiguration(), store.getTableName());
  }

  @After
  public void cleanup() throws Exception
  {
    HBaseUtil.deleteTable( store.getConfiguration(), store.getTableName());
  }


  @Test
  public void test() throws Exception
  {
    // Create DAG for testing.
    LocalMode lma = LocalMode.newInstance();

    StreamingApplication app = new StreamingApplication()
    {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
      }
    };

    DAG dag = lma.getDAG();

    // Create ActiveMQStringSinglePortOutputOperator
    MyGenerator generator = dag.addOperator( OPERATOR.GENERATOR.name(), MyGenerator.class);
    generator.setTupleNum( TUPLE_NUM );

    hbaseOutputOperator = dag.addOperator( OPERATOR.HBASEOUTPUT.name(), hbaseOutputOperator );

    hbaseInputOperator = dag.addOperator(OPERATOR.HBASEINPUT.name(), hbaseInputOperator);
    dag.setOutputPortAttribute(hbaseInputOperator.outputPort, Context.PortContext.TUPLE_CLASS, TestPOJO.class);


    TupleCacheOutputOperator output = dag.addOperator(OPERATOR.OUTPUT.name(), TupleCacheOutputOperator.class);

    // Connect ports
    dag.addStream("queue1", generator.outputPort, hbaseOutputOperator.input ).setLocality(DAG.Locality.NODE_LOCAL);
    dag.addStream("queue2", hbaseInputOperator.outputPort, output.inputPort ).setLocality(DAG.Locality.NODE_LOCAL);


    Configuration conf = new Configuration(false);
    lma.prepareDAG(app, conf);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.runAsync();

    long start = System.currentTimeMillis();
    //generator.doneLatch.await();
    while (true) {
      Thread.sleep(1000);
      logger.info("Tuple row key: ", output.getReceivedTuples());
      logger.info( "Received tuple number {}, instance is {}.", output.getReceivedTuples() == null ? 0 : output.getReceivedTuples().size(), System.identityHashCode( output ) );
      if ( output.getReceivedTuples() != null && output.getReceivedTuples().size() == TUPLE_NUM ) {
        break;
      }
      if (System.currentTimeMillis() - start > RUN_DURATION) {
        throw new RuntimeException("Testcase taking too long");
      }
    }

    lc.shutdown();
    validate( generator.getTuples(), output.getReceivedTuples() );
  }

  protected void validate( List<TestPOJO> expected, List<TestPOJO> actual )
  {
    logger.info( "expected size: " + expected.size() );
    logger.info( "actual size: " + actual.size() );
    Assert.assertTrue( String.format( "The expected size {%d} is different from actual size {%d}.", expected.size(), actual.size()  ), expected.size() == actual.size() );
    actual.removeAll(expected);
    Assert.assertTrue( "content not same.", actual.isEmpty() );
  }

  protected void setupOperators()
  {
    TableInfo<HBaseFieldInfo> tableInfo = new TableInfo<HBaseFieldInfo>();

    tableInfo.setRowOrIdExpression("row");

    List<HBaseFieldInfo> fieldsInfo = new ArrayList<HBaseFieldInfo>();
    fieldsInfo.add( new HBaseFieldInfo( "name", "name", SupportType.STRING, "f0") );
    fieldsInfo.add( new HBaseFieldInfo( "age", "age", SupportType.INTEGER, "f1") );
    fieldsInfo.add( new HBaseFieldInfo( "address", "address", SupportType.STRING, "f1") );

    tableInfo.setFieldsInfo(fieldsInfo);

    hbaseInputOperator.setTableInfo(tableInfo);
    hbaseOutputOperator.setTableInfo(tableInfo);

    store = new HBaseStore();
    store.setTableName("test");
    store.setZookeeperQuorum("localhost");
    store.setZookeeperClientPort(2181);

    hbaseInputOperator.setStore(store);
    hbaseOutputOperator.setStore(store);

    OperatorContext context = mockOperatorContext(OPERATOR_ID, new AttributeMap.DefaultAttributeMap());
    hbaseInputOperator.setup(context);
    hbaseOutputOperator.setup(context);
  }

}
