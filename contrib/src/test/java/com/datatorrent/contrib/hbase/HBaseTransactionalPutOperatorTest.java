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
package com.datatorrent.contrib.hbase;

import java.io.IOException;
import java.util.Collection;

import org.apache.hadoop.hbase.client.Put;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator.ProcessingMode;

/**
 *
 */
public class HBaseTransactionalPutOperatorTest {
  private static final Logger logger = LoggerFactory
      .getLogger(HBaseTransactionalPutOperatorTest.class);

  public HBaseTransactionalPutOperatorTest() {
  }
  @Test
  public void testAtleastOnce() throws Exception {
    try {
      //   HBaseTestHelper.startLocalCluster();
      HBaseTestHelper.clearHBase();
      TestHBasePutOperator thop = new TestHBasePutOperator();

      thop.getStore().setTableName("table1");
      thop.getStore().setZookeeperQuorum("127.0.0.1");
      thop.getStore().setZookeeperClientPort(2181);
      HBaseTuple t1=new HBaseTuple();
      t1.setColFamily("colfam0");t1.setColName("street");t1.setRow("row1");t1.setColValue("ts");
      HBaseTuple t2=new HBaseTuple();
      t2.setColFamily("colfam0");t2.setColName("city");t2.setRow("row2");t2.setColValue("tc");
      thop.setup(new OperatorContext() {

        @Override
        public <T> T getValue(Attribute<T> key) {
          if(key.equals(PROCESSING_MODE)){
            return (T) ProcessingMode.AT_LEAST_ONCE;
          }
          return key.defaultValue;
        }

        @Override
        public AttributeMap getAttributes() {
          return null;
        }

        @Override
        public int getId() {
          // TODO Auto-generated method stub
          return 0;
        }

        @Override
        public void setCounters(Object counters) {
          // TODO Auto-generated method stub

        }

        @Override
        public void sendMetrics(Collection<String> collection)
        {
        }

        @Override
        public int getWindowsFromCheckpoint()
        {
          return 0;
        }

        @Override
        public String getName()
        {
          return null;
        }
      });
      thop.beginWindow(0);
      thop.input.process(t1);
      thop.input.process(t2);
      thop.endWindow();
      HBaseTuple tuple;

      tuple = HBaseTestHelper
          .getHBaseTuple("row1", "colfam0", "street");

      Assert.assertNotNull("Tuple", tuple);
      Assert.assertEquals("Tuple row", tuple.getRow(), "row1");
      Assert.assertEquals("Tuple column family", tuple.getColFamily(), "colfam0");
      Assert.assertEquals("Tuple column name", tuple.getColName(),"street");
      Assert.assertEquals("Tuple column value", tuple.getColValue(),"ts");
    } catch (IOException e) {

      logger.error(e.getMessage());
    }
  }
  @Test
  public void testAtmostOnce1() throws Exception {
    try {
      //   HBaseTestHelper.startLocalCluster();
      HBaseTestHelper.clearHBase();
      TestHBasePutOperator thop = new TestHBasePutOperator();
      thop.getStore().setTableName("table1");
      thop.getStore().setZookeeperQuorum("127.0.0.1");
      thop.getStore().setZookeeperClientPort(2181);
      HBaseTuple t1=new HBaseTuple();
      t1.setColFamily("colfam0");t1.setColName("street");t1.setRow("row1");t1.setColValue("ts");
      HBaseTuple t2=new HBaseTuple();
      t2.setColFamily("colfam0");t2.setColName("city");t2.setRow("row2");t2.setColValue("tc");
      thop.setup(new OperatorContext() {

        @Override
        public <T> T getValue(Attribute<T> key) {
          if(key.equals(PROCESSING_MODE)){
            return (T) ProcessingMode.AT_MOST_ONCE;
          }
          return key.defaultValue;
        }

        @Override
        public AttributeMap getAttributes() {
          return null;
        }

        @Override
        public int getId() {
          // TODO Auto-generated method stub
          return 0;
        }

        @Override
        public void setCounters(Object counters) {
          // TODO Auto-generated method stub

        }

        @Override
        public void sendMetrics(Collection<String> collection)
        {
        }

        @Override
        public int getWindowsFromCheckpoint()
        {
          return 0;
        }

        @Override
        public String getName()
        {
          return null;
        }
      });
      thop.beginWindow(0);
      thop.input.process(t1);
      thop.input.process(t2);
      thop.endWindow();
      HBaseTuple tuple;

      tuple = HBaseTestHelper.getHBaseTuple("row1", "colfam0", "street");

      Assert.assertNotNull("Tuple", tuple);
      Assert.assertEquals("Tuple row", tuple.getRow(), "row1");
      Assert.assertEquals("Tuple column family", tuple.getColFamily(),
          "colfam0");
      Assert.assertEquals("Tuple column name", tuple.getColName(),
          "street");
      Assert.assertEquals("Tuple column value", tuple.getColValue(),
          "ts");
    } catch (IOException e) {

      logger.error(e.getMessage());
    }
  }
  @Test
  public void testAtmostOnce2() throws Exception {
    try {
      //   HBaseTestHelper.startLocalCluster();
      HBaseTestHelper.clearHBase();
      TestHBasePutOperator thop = new TestHBasePutOperator();
      thop.getStore().setTableName("table1");
      thop.getStore().setZookeeperQuorum("127.0.0.1");
      thop.getStore().setZookeeperClientPort(2181);
      HBaseTuple t1=new HBaseTuple();
      t1.setColFamily("colfam0");t1.setColName("street");t1.setRow("row1");t1.setColValue("ts");
      HBaseTuple t2=new HBaseTuple();
      t2.setColFamily("colfam0");t2.setColName("city");t2.setRow("row2");t2.setColValue("tc");
      thop.beginWindow(0);
      thop.input.process(t1);
      thop.setup(new OperatorContext() {

        @Override
        public <T> T getValue(Attribute<T> key) {
          if(key.equals(PROCESSING_MODE)){
            return (T) ProcessingMode.AT_MOST_ONCE;
          }
          return key.defaultValue;
        }

        @Override
        public AttributeMap getAttributes() {
          return null;
        }

        @Override
        public int getId() {
          // TODO Auto-generated method stub
          return 0;
        }

        @Override
        public void setCounters(Object counters) {
          // TODO Auto-generated method stub

        }

        @Override
        public void sendMetrics(Collection<String> collection)
        {
        }

        @Override
        public int getWindowsFromCheckpoint()
        {
          return 0;
        }

        @Override
        public String getName()
        {
          return null;
        }
      });



      thop.input.process(t2);
      thop.endWindow();
      HBaseTuple tuple,tuple2;

      tuple = HBaseTestHelper.getHBaseTuple("row1", "colfam0", "street");
      tuple2= HBaseTestHelper.getHBaseTuple("row2", "colfam0", "city");
      Assert.assertNull("Tuple", tuple);
      Assert.assertNotNull("Tuple2", tuple2);
      Assert.assertEquals("Tuple row", tuple2.getRow(), "row2");
      Assert.assertEquals("Tuple column family", tuple2.getColFamily(),"colfam0");
      Assert.assertEquals("Tuple column name", tuple2.getColName(),"city");
      Assert.assertEquals("Tuple column value", tuple2.getColValue(),"tc");
    } catch (IOException e) {

      logger.error(e.getMessage());
    }
  }

  public static class TestHBasePutOperator extends
  AbstractHBaseWindowPutOutputOperator<HBaseTuple> {

    @Override
    public Put operationPut(HBaseTuple t) throws IOException {
      Put put = new Put(t.getRow().getBytes());
      put.add(t.getColFamily().getBytes(), t.getColName().getBytes(), t.getColValue().getBytes());
      return put;
    }

  }
}
