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
package org.apache.apex.examples.pi;

import org.apache.apex.malhar.lib.io.ConsoleOutputOperator;
import org.apache.apex.malhar.lib.math.Division;
import org.apache.apex.malhar.lib.math.LogicalCompareToConstant;
import org.apache.apex.malhar.lib.math.MultiplyByConstant;
import org.apache.apex.malhar.lib.math.RunningAverage;
import org.apache.apex.malhar.lib.math.Sigma;
import org.apache.apex.malhar.lib.math.SquareCalculus;
import org.apache.apex.malhar.lib.stream.AbstractAggregator;
import org.apache.apex.malhar.lib.stream.ArrayListAggregator;
import org.apache.apex.malhar.lib.stream.Counter;
import org.apache.apex.malhar.lib.testbench.RandomEventGenerator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * <p>Calculator class.</p>
 *
 * @since 0.3.2
 */
@ApplicationAnnotation(name = "PiLibraryExample")
public class Calculator implements StreamingApplication
{
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
    comparator.setConstant(30000 * 30000);
    Counter inCircle = dag.addOperator("CountInCircle", Counter.class);
    Counter inSquare = dag.addOperator("CountInSquare", Counter.class);
    Division division = dag.addOperator("Ratio", Division.class);
    MultiplyByConstant multiplication = dag.addOperator("InstantPI", MultiplyByConstant.class);
    multiplication.setMultiplier(4);
    RunningAverage average = dag.addOperator("AveragePI", new RunningAverage());
    ConsoleOutputOperator oper = dag.addOperator("Console", new ConsoleOutputOperator());

    dag.addStream("x", xyGenerator.integer_data, squareOperator.input);
    dag.addStream("sqr", squareOperator.integerResult, pairOperator.input);
    dag.addStream("x2andy2", pairOperator.output, sumOperator.input);
    dag.addStream("x2plusy2", sumOperator.integerResult, comparator.input, inSquare.input);
    dag.addStream("inCirclePoints", comparator.greaterThan, inCircle.input);
    dag.addStream("numerator", inCircle.output, division.numerator);
    dag.addStream("denominator", inSquare.output, division.denominator);
    dag.addStream("ratio", division.doubleQuotient, multiplication.input);
    dag.addStream("instantPi", multiplication.doubleProduct, average.input);
    dag.addStream("averagePi", average.doubleAverage, oper.input);

  }

}
