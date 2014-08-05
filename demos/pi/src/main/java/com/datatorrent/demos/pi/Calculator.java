/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.demos.pi;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.HttpOutputOperator;
import com.datatorrent.lib.math.*;
import com.datatorrent.lib.stream.AbstractAggregator;
import com.datatorrent.lib.stream.ArrayListAggregator;
import com.datatorrent.lib.stream.Counter;
import com.datatorrent.lib.testbench.RandomEventGenerator;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator.InputPort;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.StreamingApplication;

/**
 * <p>Calculator class.</p>
 *
 * @since 0.3.2
 */
@ApplicationAnnotation(name="PiLibraryDemo")
public class Calculator implements StreamingApplication
{
  private final Locality locality = null;

  private InputPort<Object> getConsolePort(DAG b, String name)
  {
    // output to HTTP server when specified in environment setting
    String serverAddr = System.getenv("MALHAR_AJAXSERVER_ADDRESS");
    if (serverAddr == null) {
      ConsoleOutputOperator oper = b.addOperator(name, new ConsoleOutputOperator());
      oper.setStringFormat(name + ": %s");
      return oper.input;
    }
    HttpOutputOperator<Object> oper = b.addOperator(name, new HttpOutputOperator<Object>());
    URI u = URI.create("http://" + serverAddr + "/channel/" + name);
    oper.setResourceURL(u);
    return oper.input;
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    /* keep generating random values between 0 and 30000 */
    RandomEventGenerator xyGenerator = dag.addOperator("GenerateX", RandomEventGenerator.class);

    /* calculate square of each of the values it receives */
    SquareCalculus squareOperator = dag.addOperator("SquareX", SquareCalculus.class);

    /* pair the consecutive values */
    AbstractAggregator<Integer> pairOperator = dag.addOperator("PairXY", new ArrayListAggregator<Integer>());
    Sigma<Integer> sumOperator = dag.addOperator("SumXY", new Sigma<Integer>());
    LogicalCompareToConstant<Integer> comparator = dag.addOperator("AnalyzeLocation", new LogicalCompareToConstant<Integer>());
    comparator.setConstant(30000 *30000);
    Counter inCircle = dag.addOperator("CountInCircle", Counter.class);
    Counter inSquare = dag.addOperator("CountInSquare", Counter.class);
    Division division = dag.addOperator("Ratio", Division.class);
    MultiplyByConstant multiplication = dag.addOperator("InstantPI", MultiplyByConstant.class);
    multiplication.setMultiplier(4);
    RunningAverage average = dag.addOperator("AveragePI", new RunningAverage());

    dag.addStream("x", xyGenerator.integer_data, squareOperator.input);
    dag.addStream("sqr", squareOperator.integerResult, pairOperator.input);
    dag.addStream("x2andy2", pairOperator.output, sumOperator.input);
    dag.addStream("x2plusy2", sumOperator.integerResult, comparator.input, inSquare.input);
    dag.addStream("inCirclePoints", comparator.greaterThan, inCircle.input);
    dag.addStream("numerator", inCircle.output, division.numerator);
    dag.addStream("denominator", inSquare.output, division.denominator);
    dag.addStream("ratio", division.doubleQuotient, multiplication.input);
    dag.addStream("instantPi", multiplication.doubleProduct, average.input);
    dag.addStream("averagePi", average.doubleAverage, getConsolePort(dag, "Console"));

  }

}
