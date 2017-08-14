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
package org.apache.apex.malhar.lib.math;

import java.util.ArrayList;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.testbench.CountAndLastTupleTestSink;
import org.apache.apex.malhar.lib.util.KeyValPair;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 *
 * Functional tests for {@link org.apache.apex.malhar.lib.math.MaxKeyVal}. <p>
 *
 */
public class MaxKeyValTest
{
  /**
   * Test functional logic
   */
  @Test
  public void testNodeProcessing()
  {
    testSchemaNodeProcessing(new MaxKeyVal<String, Integer>(), "integer");
    testSchemaNodeProcessing(new MaxKeyVal<String, Double>(), "double");
    testSchemaNodeProcessing(new MaxKeyVal<String, Long>(), "long");
    testSchemaNodeProcessing(new MaxKeyVal<String, Short>(), "short");
    testSchemaNodeProcessing(new MaxKeyVal<String, Float>(), "float");
  }

  /**
   * Test operator logic emits correct results for each schema.
   *
   */
  @SuppressWarnings({ "rawtypes", "unchecked" })
  public void testSchemaNodeProcessing(MaxKeyVal oper, String type)
  {
    CountAndLastTupleTestSink maxSink = new CountAndLastTupleTestSink();
    oper.max.setSink(maxSink);

    oper.beginWindow(0);

    int numtuples = 10000;
    if (type.equals("integer")) {
      for (int i = 0; i < numtuples; i++) {
        oper.data.process(new KeyValPair("a", i));
      }
    } else if (type.equals("double")) {
      for (int i = 0; i < numtuples; i++) {
        oper.data.process(new KeyValPair("a", (double)i));
      }
    } else if (type.equals("long")) {
      for (int i = 0; i < numtuples; i++) {
        oper.data.process(new KeyValPair("a", (long)i));
      }
    } else if (type.equals("short")) {
      int count = numtuples / 1000; // cannot cross 64K
      for (short j = 0; j < count; j++) {
        oper.data.process(new KeyValPair("a", j));
      }
    } else if (type.equals("float")) {
      for (int i = 0; i < numtuples; i++) {
        oper.data.process(new KeyValPair("a", (float)i));
      }
    }

    oper.endWindow();

    Assert.assertEquals("number emitted tuples", 1, maxSink.count);
    Number val = ((KeyValPair<String, Number>)maxSink.tuple).getValue().intValue();
    if (type.equals("short")) {
      Assert.assertEquals("emitted max value was ", (new Double(numtuples / 1000 - 1)).intValue(), val);
    } else {
      Assert.assertEquals("emitted max value was ", (new Double(numtuples - 1)).intValue(), val);
    }
  }

  /**
   * Used to test partitioning.
   */
  public static class TestInputOperator extends BaseOperator implements InputOperator
  {
    public final transient DefaultOutputPort<KeyValPair<String, Integer>> output = new DefaultOutputPort<KeyValPair<String, Integer>>();
    private transient boolean first = true;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void emitTuples()
    {
      if (first) {
        for (int i = 0; i < 100; i++) {
          output.emit(new KeyValPair("a", new Integer(i)));
        }
        for (int i = 0; i < 80; i++) {
          output.emit(new KeyValPair("b", new Integer(i)));
        }
        for (int i = 0; i < 60; i++) {
          output.emit(new KeyValPair("c", new Integer(i)));
        }
        first = false;
      }
    }
  }

  public static class CollectorOperator extends BaseOperator
  {
    public static final ArrayList<KeyValPair<String, Integer>> buffer = new ArrayList<KeyValPair<String, Integer>>();
    public final transient DefaultInputPort<KeyValPair<String, Integer>> input = new DefaultInputPort<KeyValPair<String, Integer>>()
    {
      @SuppressWarnings({ "unchecked", "rawtypes" })
      @Override
      public void process(KeyValPair<String, Integer> tuple)
      {
        buffer.add(new KeyValPair(tuple.getKey(), tuple.getValue()));
      }
    };
  }
}
