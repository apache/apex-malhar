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

package org.apache.apex.malhar.stream.FunctionOperator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.function.Function;
import org.apache.apex.malhar.lib.function.FunctionOperator;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.stram.StramLocalCluster;


/**
 * Unit tests for FunctionOperator(Map, FlapMap, Filter).
 */

public class FunctionOperatorTest
{
  private static final int NumTuples = 10;
  private static final int NumFlatMapTuples = 100;
  private static final int divider = 2;
  private static final int listSize = 10;
  private static int TupleCount;
  private static int sum;

  //Sample operator to generate continuous integers in lists for FlapMap testing.
  public static class NumberListGenerator extends BaseOperator implements InputOperator
  {
    private int numMem;
    private List<Integer> nums;

    public final transient DefaultOutputPort<List<Integer>> output = new DefaultOutputPort<List<Integer>>();

    @Override
    public void setup(OperatorContext context)
    {
      numMem = 0;
      nums = new ArrayList<Integer>();
    }

    @Override
    public void emitTuples()
    {
      nums.add(numMem);
      numMem++;
      if (numMem < NumFlatMapTuples && nums.size() < listSize) {
        output.emit(nums);
        nums.clear();
      }
    }
  }

  //Sample operator to generate continuous integers for filter and map testing.
  public static class NumberGenerator extends BaseOperator implements InputOperator
  {
    private int num;

    public final transient DefaultOutputPort<Integer> output = new DefaultOutputPort<Integer>();

    @Override
    public void setup(OperatorContext context)
    {
      num = 0;
    }

    @Override
    public void emitTuples()
    {
      if (num < NumTuples) {
        output.emit(num);
        num++;
      }
    }
  }

  public static class ResultCollector extends BaseOperator
  {

    public final transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>()
    {
      @Override
      public void process(Integer in)
      {
        TupleCount++;
        sum += in;
      }
    };

    @Override
    public void setup(OperatorContext context)
    {
      TupleCount = 0;
      sum = 0;
    }

  }


  public static class FmFunction implements Function.FlatMapFunction<List<Integer>, Integer>
  {
    @Override
    public Iterable<Integer> f(List<Integer> input)
    {
      ArrayList<Integer> result = new ArrayList<Integer>();
      for (int in : input) {
        if (in % 13 == 0 || in % 17 == 0) {
          result.add(in * in);
        }
      }
      return result;
    }
  }

  public static class Square implements Function.MapFunction<Integer, Integer>
  {
    @Override
    public Integer f(Integer input)
    {
      return input * input;
    }
  }


  @Test
  public void testMapOperator() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    NumberGenerator numGen = dag.addOperator("numGen", new NumberGenerator());
    FunctionOperator.MapFunctionOperator<Integer, Integer> mapper
        = dag.addOperator("mapper", new FunctionOperator.MapFunctionOperator<Integer, Integer>(new Square()));
    ResultCollector collector = dag.addOperator("collector", new ResultCollector());

    dag.addStream("raw numbers", numGen.output, mapper.input);
    dag.addStream("mapped results", mapper.output, collector.input);

    // Create local cluster
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        return TupleCount == NumTuples;
      }
    });

    lc.run(5000);

    Assert.assertEquals(sum, 285);
  }

  @Test
  public void testMapOperatorStream() throws Exception
  {
    NumberGenerator numGen = new NumberGenerator();
    ResultCollector collector = new ResultCollector();

    ApexStream<Integer> nums = StreamFactory.fromInput(numGen, numGen.output)
        .map(new Square());

    nums.addOperator(collector, collector.input, null)
        .runEmbedded(false, 10000, new Callable<Boolean>()
        {
          @Override
          public Boolean call() throws Exception
          {
            return TupleCount == NumTuples;
          }
        });

    Assert.assertEquals(sum, 285);
  }


  @Test
  public void testFlatMapOperator() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    NumberListGenerator numGen = dag.addOperator("numGen", new NumberListGenerator());
    FunctionOperator.FlatMapFunctionOperator<List<Integer>, Integer> fm
        = dag.addOperator("flatmap", new FunctionOperator.FlatMapFunctionOperator<>(new FmFunction()));
    ResultCollector collector = dag.addOperator("collector", new ResultCollector());

    dag.addStream("raw numbers", numGen.output, fm.input);
    dag.addStream("flatmap results", fm.output, collector.input);

    // Create local cluster
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        return TupleCount == 13;
      }
    });

    lc.run(5000);

    Assert.assertEquals(sum, 39555);
  }

  @Test
  public void testFlatMapOperatorStream() throws Exception
  {
    NumberListGenerator numGen = new NumberListGenerator();
    ResultCollector collector = new ResultCollector();

    ApexStream<Integer> numLists = StreamFactory.fromInput(numGen, numGen.output)
        .flatMap(new FmFunction());
    numLists.addOperator(collector, collector.input, null)
        .runEmbedded(false, 10000, new Callable<Boolean>()
        {
          @Override
          public Boolean call() throws Exception
          {
            return TupleCount == 13;
          }
        });
    Assert.assertEquals(sum, 39555);
  }

  @Test
  public void testFilterOperator() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    FunctionOperator.FilterFunctionOperator<Integer> filter0
        = new FunctionOperator.FilterFunctionOperator<Integer>(new Function.FilterFunction<Integer>()
        {
          @Override
          public boolean f(Integer in)
          {
            return in % divider == 0;
          }
        });

    NumberGenerator numGen = dag.addOperator("numGen", new NumberGenerator());
    FunctionOperator.FilterFunctionOperator<Integer> filter = dag.addOperator("filter", filter0);
    ResultCollector collector = dag.addOperator("collector", new ResultCollector());

    dag.addStream("raw numbers", numGen.output, filter.input);
    dag.addStream("filtered results", filter.output, collector.input);

    // Create local cluster
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);

    ((StramLocalCluster)lc).setExitCondition(new Callable<Boolean>()
    {
      @Override
      public Boolean call() throws Exception
      {
        return TupleCount == NumTuples / divider;
      }
    });

    lc.run(5000);
    Assert.assertEquals(sum, 20);
  }

  @Test
  public void testFilterOperatorStream() throws Exception
  {
    NumberGenerator numGen = new NumberGenerator();
    ResultCollector collector = new ResultCollector();

    ApexStream<Integer> nums = StreamFactory.fromInput(numGen, numGen.output)
        .filter(new Function.FilterFunction<Integer>()
        {
          @Override
          public boolean f(Integer in)
          {
            return in % divider == 0;
          }
        });

    nums.addOperator(collector, collector.input, null)
        .runEmbedded(false, 10000, new Callable<Boolean>()
        {
          @Override
          public Boolean call() throws Exception
          {
            return TupleCount == NumTuples / divider;
          }
        });

    Assert.assertEquals(sum, 20);
  }


}
