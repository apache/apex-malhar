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
package com.datatorrent.demos.iteration;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.common.util.SimpleDelayOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.testbench.RandomEventGenerator;

/**
 * Iteration demo : <br>
 *
 * <pre>
 * LocalMode.runApp(new Application(), 600000); // 10 min run
 * </pre>
 *
 * Run Success : <br>
 * For successful deployment and run, user should see the Fibonacci sequence, something like the
 * following output on the console:
 *
 * <pre>
 * 1
 * 1
 * 2
 * 3
 * 5
 * 8
 * 13
 * 21
 * 34
 * 55
 * ...
 * </pre>
 *
 */
@ApplicationAnnotation(name="IterationDemo")
public class Application implements StreamingApplication
{
  public static class FibonacciOperator extends BaseOperator
  {
    public long currentNumber = 1;
    private transient long tempNum;
    public transient DefaultInputPort<Object> dummyInputPort = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
      }
    };
    public transient DefaultInputPort<Long> input = new DefaultInputPort<Long>()
    {
      @Override
      public void process(Long tuple)
      {
        tempNum = tuple;
      }
    };
    public transient DefaultOutputPort<Long> output = new DefaultOutputPort<>();


    @Override
    public void endWindow()
    {
      output.emit(currentNumber);
      currentNumber += tempNum;
    }
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomEventGenerator rand = dag.addOperator("rand", new RandomEventGenerator());
    FibonacciOperator fib = dag.addOperator("FIB", FibonacciOperator.class);
    SimpleDelayOperator opDelay = dag.addOperator("opDelay", SimpleDelayOperator.class);
    ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("dummy_to_operator", rand.integer_data, fib.dummyInputPort);
    dag.addStream("operator_to_delay", fib.output, opDelay.input, console.input);
    dag.addStream("delay_to_operator", opDelay.output, fib.input);
  }

}
