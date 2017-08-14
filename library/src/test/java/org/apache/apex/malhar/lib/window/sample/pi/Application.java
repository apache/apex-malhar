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
package org.apache.apex.malhar.lib.window.sample.pi;

import org.joda.time.Duration;

import org.apache.apex.malhar.lib.io.ConsoleOutputOperator;
import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.WindowState;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedStorage;
import org.apache.apex.malhar.lib.window.impl.WindowedOperatorImpl;
import org.apache.commons.lang3.mutable.MutableLong;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is an example of using the WindowedOperator concepts to calculate the value of pi.
 */
public class Application implements StreamingApplication
{
  public static class RandomNumberPairGenerator extends BaseOperator implements InputOperator
  {
    public final transient DefaultOutputPort<Tuple<MutablePair<Double, Double>>> output = new DefaultOutputPort<>();

    @Override
    public void emitTuples()
    {
      Tuple.PlainTuple<MutablePair<Double, Double>> tuple = new Tuple.PlainTuple<>(new MutablePair<>(Math.random(), Math.random()));
      this.output.emit(tuple);
    }
  }

  public static class PiAccumulation implements Accumulation<MutablePair<Double, Double>, MutablePair<MutableLong, MutableLong>, Double>
  {
    @Override
    public MutablePair<MutableLong, MutableLong> defaultAccumulatedValue()
    {
      return new MutablePair<>(new MutableLong(0), new MutableLong(0));
    }

    @Override
    public MutablePair<MutableLong, MutableLong> accumulate(MutablePair<MutableLong, MutableLong> accumulatedValue, MutablePair<Double, Double> input)
    {
      if (input.getLeft() * input.getLeft() + input.getRight() * input.getRight() < 1) {
        accumulatedValue.getLeft().increment();
      }
      accumulatedValue.getRight().increment();
      return accumulatedValue;
    }

    @Override
    public MutablePair<MutableLong, MutableLong> merge(MutablePair<MutableLong, MutableLong> accumulatedValue1, MutablePair<MutableLong, MutableLong> accumulatedValue2)
    {
      accumulatedValue1.getLeft().add(accumulatedValue2.getLeft());
      accumulatedValue1.getRight().add(accumulatedValue2.getRight());
      return accumulatedValue1;
    }

    @Override
    public Double getOutput(MutablePair<MutableLong, MutableLong> accumulatedValue)
    {
      return accumulatedValue.getRight().longValue() == 0 ? 0.0 : (((double)accumulatedValue.getLeft().longValue()) * 4 / accumulatedValue.getRight().longValue());
    }

    @Override
    public Double getRetraction(Double value)
    {
      return -value;
    }
  }

  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    RandomNumberPairGenerator inputOperator = new RandomNumberPairGenerator();
    WindowedOperatorImpl<MutablePair<Double, Double>, MutablePair<MutableLong, MutableLong>, Double> windowedOperator = new WindowedOperatorImpl<>();
    Accumulation<MutablePair<Double, Double>, MutablePair<MutableLong, MutableLong>, Double> piAccumulation = new PiAccumulation();

    windowedOperator.setAccumulation(piAccumulation);
    windowedOperator.setDataStorage(new InMemoryWindowedStorage<MutablePair<MutableLong, MutableLong>>());
    windowedOperator.setWindowStateStorage(new InMemoryWindowedStorage<WindowState>());
    windowedOperator.setWindowOption(new WindowOption.GlobalWindow());
    windowedOperator.setTriggerOption(TriggerOption.AtWatermark().withEarlyFiringsAtEvery(Duration.millis(1000)).accumulatingFiredPanes());

    ConsoleOutputOperator outputOperator = new ConsoleOutputOperator();
    dag.addOperator("inputOperator", inputOperator);
    dag.addOperator("windowedOperator", windowedOperator);
    dag.addOperator("outputOperator", outputOperator);
    dag.addStream("input_windowed", inputOperator.output, windowedOperator.input);
    dag.addStream("windowed_output", windowedOperator.output, outputOperator.input);
  }

  public static void main(String[] args) throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    lma.prepareDAG(new Application(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.run();
  }
}
