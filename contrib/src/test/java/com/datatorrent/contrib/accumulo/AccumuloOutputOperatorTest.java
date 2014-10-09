/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.accumulo;

import junit.framework.Assert;

import org.apache.accumulo.core.data.Mutation;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context.OperatorContext;

public class AccumuloOutputOperatorTest {
  private static final Logger logger = LoggerFactory
      .getLogger(AccumuloOutputOperatorTest.class);

  @Test
  public void testPut() throws Exception {

    AccumuloTestHelper.getConnector();
    AccumuloTestHelper.clearTable();
    TestAccumuloOutputOperator atleastOper = new TestAccumuloOutputOperator();

    atleastOper.getStore().setTableName("tab1");
    atleastOper.getStore().setZookeeperHost("127.0.0.1");
    atleastOper.getStore().setInstanceName("instance");
    atleastOper.getStore().setUserName("root");
    atleastOper.getStore().setPassword("pass");

    atleastOper.setup(new OperatorContext() {

      @Override
      public <T> T getValue(Attribute<T> key) {
        return null;
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
    });
    atleastOper.beginWindow(0);
    AccumuloTuple a=new AccumuloTuple();
    a.setRow("john");a.setColFamily("colfam0");a.setColName("street");a.setColValue("patrick");
    atleastOper.input.process(a);
    atleastOper.endWindow();
    AccumuloTuple tuple;

    tuple = AccumuloTestHelper
        .getAccumuloTuple("john", "colfam0", "street");

    Assert.assertNotNull("Tuple", tuple);
    Assert.assertEquals("Tuple row", tuple.getRow(), "john");
    Assert.assertEquals("Tuple column family", tuple.getColFamily(),"colfam0");
    Assert.assertEquals("Tuple column name", tuple.getColName(),"street");
    Assert.assertEquals("Tuple column value", tuple.getColValue(),"patrick");

  }
  public static class TestAccumuloOutputOperator extends AbstractAccumuloOutputOperator<AccumuloTuple> {

    @Override
    public Mutation operationMutation(AccumuloTuple t) {
      Mutation mutation = new Mutation(t.getRow().getBytes());
      mutation.put(t.getColFamily().getBytes(),t.getColName().getBytes(),t.getColValue().getBytes());
      return mutation;
    }

  }

}
