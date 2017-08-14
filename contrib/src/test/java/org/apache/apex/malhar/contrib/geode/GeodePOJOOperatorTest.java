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
package org.apache.apex.malhar.contrib.geode;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.apex.malhar.contrib.util.TestPOJO;
import org.apache.apex.malhar.contrib.util.TupleGenerator;
import org.apache.apex.malhar.lib.util.TableInfo;

import com.datatorrent.netlet.util.DTThrowable;

@SuppressWarnings("rawtypes")
public class GeodePOJOOperatorTest
{
  public static final int TUPLE_SIZE = 10;

  private GeodeStore store;

  @Before
  public void setup()
  {
    store = new GeodeStore();
    store.setLocatorHost("192.168.1.128");
    if (System.getProperty("dev.locator.connection") != null) {
      store.setLocatorHost(System.getProperty("dev.locator.connection"));
    }
    store.setLocatorPort(10334);
    store.setRegionName("operator5");
  }

  public void cleanup()
  {
    if (store != null) {
      try {
        store.disconnect();
      } catch (Exception e) {
        DTThrowable.rethrow(e);
      }
    }

  }

  @SuppressWarnings("unchecked")
  @Test
  public void testGeodeOutputOperatorInternal() throws Exception
  {
    GeodePOJOOutputOperator operator = new GeodePOJOOutputOperator();
    operator.setStore(store);

    TableInfo tableInfo = new TableInfo();
    tableInfo.setRowOrIdExpression(TestPOJO.getRowExpression());
    tableInfo.setFieldsInfo(TestPOJO.getFieldsInfo());
    tableInfo.setRowOrIdExpression(TestPOJO.getRowExpression());
    operator.setTableInfo(tableInfo);

    operator.setup(null);

    TupleGenerator<TestPOJO> generator = new TupleGenerator<TestPOJO>(TestPOJO.class);

    for (int i = 0; i < TUPLE_SIZE; ++i) {
      operator.processTuple(generator.getNextTuple());
    }

    generator.reset();

    for (int i = 0; i < TUPLE_SIZE; ++i) {
      operator.processTuple(generator.getNextTuple());
    }

    //    readDataAndVerify(operator.getStore(), generator);
  }

  public void readDataAndVerify(GeodeStore store, TupleGenerator<TestPOJO> generator)
  {
    generator.reset();

    for (int i = 0; i < TUPLE_SIZE; ++i) {
      TestPOJO expected = generator.getNextTuple();
      TestPOJO read = (TestPOJO)store.get(expected.getRow());
      Assert.assertTrue(String.format("expected={%s}, actually={%s}", expected.toString(), read.toString()),
          expected.completeEquals(read));
    }
  }
}
