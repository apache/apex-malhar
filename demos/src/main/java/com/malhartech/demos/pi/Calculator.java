/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.pi;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.Operator.OutputPort;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.math.*;
import com.malhartech.lib.stream.AbstractAggregator;
import com.malhartech.lib.stream.ArrayListAggregator;
import com.malhartech.lib.stream.Counter;
import com.malhartech.lib.testbench.RandomEventGenerator;
import java.util.Collection;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Calculator implements ApplicationFactory
{
  @Override
  @SuppressWarnings("unchecked")
  public DAG getApplication(Configuration conf)
  {
    DAG dag = new DAG(conf);

    /* keep generating random values between 0 and 30000 */
    RandomEventGenerator xyGenerator = dag.addOperator("GenerateX", RandomEventGenerator.class);
    xyGenerator.setMinvalue(0);
    xyGenerator.setMaxcountofwindows(30000);

    /* calculate square of each of the values it receives */
    IntegerSquareCalculus squareOperator = dag.addOperator("SquareX", IntegerSquareCalculus.class);

    /* pair the consecutive values */
    AbstractAggregator<Long> pairOperator = dag.addOperator("PairXY", new ArrayListAggregator<Long>());
    pairOperator.setSize(2);

    IntegerSumCalculus sumOperator = dag.addOperator("SumXY", IntegerSumCalculus.class);

    LogicalCompareToConstant<Long> comparator = dag.addOperator("AnalyzeLocation", new LogicalCompareToConstant<Long>());
    comparator.setConstant(30000 * 30000L);

    Counter inCircle = dag.addOperator("CountInCircle", Counter.class);
    Counter inSquare = dag.addOperator("CountInSquare", Counter.class);

    Division division = dag.addOperator("Ratio", Division.class);

    MultiplyByConstant multiplication = dag.addOperator("InstantPI", MultiplyByConstant.class);
    multiplication.setMultiplier(4);

    ConsoleOutputOperator<Double> console = dag.addOperator("Console", new ConsoleOutputOperator<Double>());
    console.setStringFormat("instantPI = %.2d");

    dag.addStream("x", xyGenerator.integer_data, squareOperator.input);
    dag.addStream("sqr(x)", squareOperator.output, pairOperator.input);

    DefaultOutputPort pairOutput = pairOperator.output;
    dag.addStream("x2,y2", (OutputPort<Collection<Integer>>)pairOutput, sumOperator.input, inSquare.input);

    dag.addStream("x2+y2", sumOperator.output, comparator.input);

    dag.addStream("inCirclePoints", comparator.lessThanOrEqualTo, inCircle.input);

    dag.addStream("numerator", inCircle.output, division.numerator);
    dag.addStream("denominator", inSquare.output, division.denominator);

    dag.addStream("ratio", division.doubleQuotient, multiplication.input);

    dag.addStream("instantPi", multiplication.doubleProduct, console.input);

    return dag;
  }

}
