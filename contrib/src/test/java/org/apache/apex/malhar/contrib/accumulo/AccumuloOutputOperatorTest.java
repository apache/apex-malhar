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
package org.apache.apex.malhar.contrib.accumulo;

import org.junit.Assert;
import org.junit.Test;

import org.apache.accumulo.core.data.Mutation;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

public class AccumuloOutputOperatorTest
{
  @Test
  public void testPut() throws Exception
  {

    AccumuloTestHelper.getConnector();
    AccumuloTestHelper.clearTable();
    TestAccumuloOutputOperator atleastOper = new TestAccumuloOutputOperator();

    atleastOper.getStore().setTableName("tab1");
    atleastOper.getStore().setZookeeperHost("127.0.0.1");
    atleastOper.getStore().setInstanceName("instance");
    atleastOper.getStore().setUserName("root");
    atleastOper.getStore().setPassword("pass");

    atleastOper.setup(mockOperatorContext(0));
    atleastOper.beginWindow(0);
    AccumuloTuple a = new AccumuloTuple();
    a.setRow("john");
    a.setColFamily("colfam0");
    a.setColName("street");
    a.setColValue("patrick");
    atleastOper.input.process(a);
    atleastOper.endWindow();
    AccumuloTuple tuple;

    tuple = AccumuloTestHelper
        .getAccumuloTuple("john", "colfam0", "street");

    Assert.assertNotNull("Tuple", tuple);
    Assert.assertEquals("Tuple row", tuple.getRow(), "john");
    Assert.assertEquals("Tuple column family", tuple.getColFamily(),"colfam0");
    Assert.assertEquals("Tuple column name", tuple.getColName(),"street");
    Assert.assertEquals("Tuple column value", tuple.getColValue(), "patrick");

  }

  public static class TestAccumuloOutputOperator extends AbstractAccumuloOutputOperator<AccumuloTuple>
  {
    @Override
    public Mutation operationMutation(AccumuloTuple t)
    {
      Mutation mutation = new Mutation(t.getRow().getBytes());
      mutation.put(t.getColFamily().getBytes(),t.getColName().getBytes(),t.getColValue().getBytes());
      return mutation;
    }

  }

}
