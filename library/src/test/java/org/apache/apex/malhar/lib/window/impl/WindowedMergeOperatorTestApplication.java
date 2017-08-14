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
package org.apache.apex.malhar.lib.window.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.joda.time.Duration;

import org.apache.apex.malhar.lib.io.ConsoleOutputOperator;
import org.apache.apex.malhar.lib.window.ControlTuple;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.WindowState;
import org.apache.apex.malhar.lib.window.WindowedStorage;
import org.apache.apex.malhar.lib.window.accumulation.CoGroup;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;

/**
 * Example application to show usage of Windowed Merge Operator. Two generators send out two streams of integers,
 * the Merge operator will do a co-group to Merge all income tuples and send output to collector and output to console.
 * User can choose different window options and see how the application behaves.
 */
public class WindowedMergeOperatorTestApplication implements StreamingApplication
{
  private static WindowedStorage.WindowedPlainStorage<WindowState> windowStateMap = new InMemoryWindowedStorage<>();
  private static final long windowDuration = 1000;


  public static Window.TimeWindow assignTestWindow(long timestamp)
  {
    long beginTimestamp = timestamp - timestamp % windowDuration;
    Window.TimeWindow window = new Window.TimeWindow(beginTimestamp, windowDuration);
    if (!windowStateMap.containsWindow(window)) {
      windowStateMap.put(window, new WindowState());
    }
    return window;
  }

  public static class NumGen1 extends BaseOperator implements InputOperator
  {
    private int i;
    private long watermarkTime;
    private long startingTime;

    public final transient DefaultOutputPort<Tuple.WindowedTuple<Integer>> output = new DefaultOutputPort<>();
    public final transient DefaultOutputPort<ControlTuple> watermarkDefaultOutputPort = new DefaultOutputPort<>();

    @Override
    public void setup(Context.OperatorContext context)
    {
      super.setup(context);
      startingTime = System.currentTimeMillis();
      watermarkTime = System.currentTimeMillis() + 10000;
      i = 1;
    }

    @Override
    public void emitTuples()
    {
      while (i <= 20) {
        if (System.currentTimeMillis() - startingTime >= (i + 1) * 400) {
          output.emit(new Tuple.WindowedTuple<Integer>(assignTestWindow(System.currentTimeMillis()), i));
          i++;
        }
      }
    }

    @Override
    public void endWindow()
    {
      if (i <= 20) {
        watermarkDefaultOutputPort.emit(new WatermarkImpl(watermarkTime));
      }
    }
  }

  public static class NumGen2 extends BaseOperator implements InputOperator
  {
    private int i;
    private long watermarkTime;
    private long startingTime;

    public final transient DefaultOutputPort<Tuple.WindowedTuple<Integer>> output = new DefaultOutputPort<>();
    public final transient DefaultOutputPort<ControlTuple> watermarkDefaultOutputPort = new DefaultOutputPort<>();


    @Override
    public void setup(Context.OperatorContext context)
    {
      super.setup(context);
      startingTime = System.currentTimeMillis();
      watermarkTime = System.currentTimeMillis() + 10000;
      i = 1;
    }

    @Override
    public void emitTuples()
    {
      while (i <= 20) {
        if (System.currentTimeMillis() - startingTime >= (i + 1) * 400) {
          output.emit(new Tuple.WindowedTuple<Integer>(assignTestWindow(System.currentTimeMillis()), 10 * i));
          i++;
        }
      }
    }

    @Override
    public void endWindow()
    {
      if (i <= 20) {
        watermarkDefaultOutputPort.emit(new WatermarkImpl(watermarkTime));
      }
    }
  }

  public static class Collector extends BaseOperator
  {
    public static List<List<List<Integer>>> result = new ArrayList<>();

    public final transient DefaultOutputPort<Tuple<List<List<Integer>>>> output = new DefaultOutputPort<>();

    public final transient DefaultInputPort<Tuple<List<List<Integer>>>> input = new DefaultInputPort<Tuple<List<List<Integer>>>>()
    {
      @Override
      public void process(Tuple<List<List<Integer>>> tuple)
      {
        result.add(tuple.getValue());
        output.emit(tuple);
      }
    };
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    WindowedMergeOperatorImpl<Integer, Integer, List<Set<Integer>>, List<List<Integer>>> op
        = dag.addOperator("Merge", new WindowedMergeOperatorImpl<Integer, Integer, List<Set<Integer>>, List<List<Integer>>>());
    op.setAccumulation(new CoGroup<Integer>());
    op.setDataStorage(new InMemoryWindowedStorage<List<Set<Integer>>>());
    op.setRetractionStorage(new InMemoryWindowedStorage<List<List<Integer>>>());
    op.setWindowStateStorage(windowStateMap);

    // Can select one of the following window options, or don't select any of them.
    //op.setWindowOption(new WindowOption.GlobalWindow());
    op.setWindowOption(new WindowOption.TimeWindows(Duration.millis(2000)));

    op.setTriggerOption(new TriggerOption().withEarlyFiringsAtEvery(1).accumulatingFiredPanes());
    op.setAllowedLateness(Duration.millis(500));

    NumGen1 numGen1 = dag.addOperator("numGen1", new NumGen1());
    NumGen2 numGen2 = dag.addOperator("numGen2", new NumGen2());

    Collector collector = dag.addOperator("collector", new Collector());
    ConsoleOutputOperator con = dag.addOperator("console", new ConsoleOutputOperator());

    dag.addStream("num1", numGen1.output, op.input);
    dag.addStream("num2", numGen2.output, op.input2);
    dag.addStream("wm1", numGen1.watermarkDefaultOutputPort, op.controlInput);
    dag.addStream("wm2", numGen2.watermarkDefaultOutputPort, op.controlInput2);

    dag.addStream("MergedResult", op.output, collector.input);
    dag.addStream("output", collector.output, con.input);
  }

  public static void main(String[] args) throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    lma.prepareDAG(new WindowedMergeOperatorTestApplication(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.run(20000);
  }
}
