/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.pi;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.api.Operator.InputPort;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.io.HttpOutputOperator;
import com.malhartech.lib.math.*;
import com.malhartech.lib.stream.AbstractAggregator;
import com.malhartech.lib.stream.ArrayListAggregator;
import com.malhartech.lib.stream.Counter;
import com.malhartech.lib.testbench.RandomEventGenerator;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Calculator implements ApplicationFactory
{
  private final boolean allInline = false;

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
  public DAG getApplication(Configuration conf)
  {
    DAG dag = new DAG(conf);

    /* keep generating random values between 0 and 30000 */
    RandomEventGenerator xyGenerator = dag.addOperator("GenerateX", RandomEventGenerator.class);
    xyGenerator.setMinvalue(0);
    xyGenerator.setMaxvalue(30000);

    /* calculate square of each of the values it receives */
    SquareCalculus squareOperator = dag.addOperator("SquareX", SquareCalculus.class);

    /* pair the consecutive values */
    AbstractAggregator<Integer> pairOperator = dag.addOperator("PairXY", new ArrayListAggregator<Integer>());
    pairOperator.setSize(2);

    Sigma<Integer> sumOperator = dag.addOperator("SumXY", new Sigma<Integer>());

    LogicalCompareToConstant<Integer> comparator = dag.addOperator("AnalyzeLocation", new LogicalCompareToConstant<Integer>());
    comparator.setConstant(30000 * 30000);

    Counter inCircle = dag.addOperator("CountInCircle", Counter.class);
    Counter inSquare = dag.addOperator("CountInSquare", Counter.class);

    Division division = dag.addOperator("Ratio", Division.class);

    MultiplyByConstant multiplication = dag.addOperator("InstantPI", MultiplyByConstant.class);
    multiplication.setMultiplier(4);

    RunningAverage average = dag.addOperator("AveragePI", new RunningAverage());

    dag.addStream("x", xyGenerator.integer_data, squareOperator.input).setInline(allInline);
    dag.addStream("sqr", squareOperator.integerResult, pairOperator.input).setInline(allInline);
    dag.addStream("x2andy2", pairOperator.output, sumOperator.input).setInline(allInline);
    dag.addStream("x2plusy2", sumOperator.integerResult, comparator.input, inSquare.input).setInline(allInline);
    dag.addStream("inCirclePoints", comparator.greaterThan, inCircle.input).setInline(allInline);
    dag.addStream("numerator", inCircle.output, division.numerator).setInline(allInline);
    dag.addStream("denominator", inSquare.output, division.denominator).setInline(allInline);
    dag.addStream("ratio", division.doubleQuotient, multiplication.input).setInline(allInline);
    dag.addStream("instantPi", multiplication.doubleProduct, average.input).setInline(allInline);
    dag.addStream("averagePi", average.doubleAverage, getConsolePort(dag, "Console")).setInline(allInline);

    return dag;
  }

}
