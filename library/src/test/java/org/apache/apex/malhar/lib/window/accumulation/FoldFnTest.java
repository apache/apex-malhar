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
package org.apache.apex.malhar.lib.window.accumulation;

import org.junit.Assert;
import org.junit.Test;
import org.apache.apex.malhar.lib.window.Tuple;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

/**
 * Test for {@link ReduceFn}.
 */
public class FoldFnTest
{
  public static class NumGen extends BaseOperator implements InputOperator
  {
    public transient DefaultOutputPort<Integer> output = new DefaultOutputPort<>();

    public static int count = 0;
    private int i = 0;

    public NumGen()
    {
      count = 0;
      i = 0;
    }

    @Override
    public void emitTuples()
    {
      while (i <= 7) {
        try {
          Thread.sleep(50);
        } catch (InterruptedException e) {
          // Ignore it.
        }
        count++;
        if (i >= 0) {
          output.emit(i++);
        }
      }
      i = -1;
    }
  }

  public static class Collector extends BaseOperator
  {
    private static int result;

    public transient DefaultInputPort<Tuple.WindowedTuple<Integer>> input = new DefaultInputPort<Tuple.WindowedTuple<Integer>>()
    {
      @Override
      public void process(Tuple.WindowedTuple<Integer> tuple)
      {
        result = tuple.getValue();
      }
    };

    public int getResult()
    {
      return result;
    }
  }

  public static class Plus extends FoldFn<Integer, Integer>
  {
    @Override
    public Integer merge(Integer accumulatedValue1, Integer accumulatedValue2)
    {
      return fold(accumulatedValue1, accumulatedValue2);
    }

    @Override
    public Integer fold(Integer input1, Integer input2)
    {
      if (input1 == null) {
        return input2;
      }
      return input1 + input2;
    }
  }

  @Test
  public void FoldFnTest()
  {

    FoldFn<String, String> concat = new FoldFn<String, String>()
    {
      @Override
      public String merge(String accumulatedValue1, String accumulatedValue2)
      {
        return fold(accumulatedValue1, accumulatedValue2);
      }

      @Override
      public String fold(String input1, String input2)
      {
        return input1 + ", " + input2;
      }
    };

    String[] ss = new String[]{"b", "c", "d", "e"};
    String base = "a";

    for (String s : ss) {
      base = concat.accumulate(base, s);
    }
    Assert.assertEquals("a, b, c, d, e", base);
  }
}
