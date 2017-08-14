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

package org.apache.apex.malhar.lib.filter;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CountTestSink;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context;
import com.datatorrent.stram.engine.PortContext;

/**
 * Tests for FilterOperator
 */
public class FilterTest
{
  public static class DummyPrivatePOJO
  {
    private long val;

    public long getVal()
    {
      return val;
    }

    public void setVal(long val)
    {
      this.val = val;
    }
  }

  public static class DummyPublicPOJO
  {
    public long val;
  }


  private static FilterOperator filter;
  private static DummyPrivatePOJO data;
  private static DummyPublicPOJO pdata;

  private static CountTestSink<Object> trueSink;
  private static CountTestSink<Object> falseSink;
  private static CountTestSink<Object> errorSink;

  public void clearSinks()
  {
    trueSink.clear();
    falseSink.clear();
    errorSink.clear();
  }

  public void prepareFilterOperator(Class<?> inClass, String condition)
  {
    filter.truePort.setSink(trueSink);
    filter.falsePort.setSink(falseSink);
    filter.error.setSink(errorSink);

    filter.setup(null);

    Attribute.AttributeMap in = new Attribute.AttributeMap.DefaultAttributeMap();
    in.put(Context.PortContext.TUPLE_CLASS, inClass);
    filter.input.setup(new PortContext(in, null));

    filter.setCondition(condition);

    filter.activate(null);
  }

  public void clearFilterOperator()
  {
    clearSinks();

    filter.deactivate();
    filter.teardown();
  }

  @Test
  public void testFilterPrivate()
  {
    prepareFilterOperator(DummyPrivatePOJO.class, "({$}.getVal() == 100)");

    filter.beginWindow(0);

    data.setVal(100);
    filter.input.put(data);
    Assert.assertEquals("true condition true tuples", 1, trueSink.getCount());
    Assert.assertEquals("true condition false tuples", 0, falseSink.getCount());
    Assert.assertEquals("true condition error tuples", 0, errorSink.getCount());

    filter.endWindow();

    clearSinks();

    /* when condition is not true */
    filter.beginWindow(1);

    data.setVal(1000);
    filter.input.put(data);
    Assert.assertEquals("false condition true tuples", 0, trueSink.getCount());
    Assert.assertEquals("false condition false tuples", 1, falseSink.getCount());
    Assert.assertEquals("false condition error tuples", 0, errorSink.getCount());

    filter.endWindow();

    clearFilterOperator();
  }

  @Test
  public void testFilterPublic()
  {
    prepareFilterOperator(DummyPublicPOJO.class, "({$}.val == 100)");

    /* when condition is true */
    filter.beginWindow(0);

    pdata.val = 100;
    filter.input.put(pdata);
    Assert.assertEquals("true condition true tuples", 1, trueSink.getCount());
    Assert.assertEquals("true condition false tuples", 0, falseSink.getCount());
    Assert.assertEquals("true condition error tuples", 0, errorSink.getCount());

    filter.endWindow();

    clearSinks();

    /* when condition is not true */
    filter.beginWindow(1);

    pdata.val = 1000;
    filter.input.put(pdata);
    Assert.assertEquals("false condition true tuples", 0, trueSink.getCount());
    Assert.assertEquals("false condition false tuples", 1, falseSink.getCount());
    Assert.assertEquals("false condition error tuples", 0, errorSink.getCount());

    filter.endWindow();

    clearFilterOperator();
  }

  @Test
  public void testFilterError()
  {
    prepareFilterOperator(DummyPublicPOJO.class, "({$}.val == 1)");

    filter.beginWindow(0);

    filter.input.put(data);
    Assert.assertEquals("error condition true tuples", 0, trueSink.getCount());
    Assert.assertEquals("error condition false tuples", 0, falseSink.getCount());
    Assert.assertEquals("error condition error tuples", 1, errorSink.getCount());

    filter.endWindow();

    clearFilterOperator();
  }

  @Test
  public void testOptionalExpressionFunctions()
  {
    filter.setAdditionalExpressionFunctions(Arrays.asList(new String[] {"org.apache.commons.lang3.BooleanUtils.*"}));
    prepareFilterOperator(DummyPublicPOJO.class, "({$}.val == 1)");
    Assert.assertEquals(6, filter.getExpressionFunctions().size());
  }

  @Test
  public void testSetOptionalExpressionFunctionsItem()
  {
    filter.setOptionalExpressionFunctionsItem(10,"org.apache.commons.lang3.BooleanUtils.*");
    prepareFilterOperator(DummyPublicPOJO.class, "({$}.val == 1)");
    Assert.assertEquals(6, filter.getExpressionFunctions().size());
  }


  @Before
  public void setup()
  {
    data = new DummyPrivatePOJO();
    pdata = new DummyPublicPOJO();
    filter = new FilterOperator();

    trueSink = new CountTestSink<>();
    falseSink = new CountTestSink<>();
    errorSink = new CountTestSink<>();
  }
}
