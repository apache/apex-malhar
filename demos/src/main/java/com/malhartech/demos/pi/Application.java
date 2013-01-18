/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.pi;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DAG;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.testbench.RandomEventGenerator;
import org.apache.hadoop.conf.Configuration;

/**
 * This is an an example of DAG which can be used to calculate value of PI.
 *
 // describe the algorithm here.
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Application implements ApplicationFactory
{
  private final boolean allInline = true;

  @Override
  public DAG getApplication(Configuration conf)
  {
    int maxValue = 30000;

    DAG dag = new DAG(conf);

    /* add all the input operators (spouts) and operators (bolts) to the dag */
//    SquareNumberSpout randOperator1 = dag.addOperator("rand1", new SquareNumberSpout());
//    SquareNumberSpout randOperator2 = dag.addOperator("rand2", new SquareNumberSpout());
    RandomEventGenerator randOperator1 = dag.addOperator("rand1", new RandomEventGenerator());
    RandomEventGenerator randOperator2 = dag.addOperator("rand2", new RandomEventGenerator());
    SumCompareBolt sumOperator = dag.addOperator("sum", new SumCompareBolt());
//    SumCompareBolt exclaim2Operator = dag.addOperator("exclaim2", new SumCompareBolt());
    randOperator1.setMinvalue(0);
    randOperator1.setMaxvalue(maxValue);
    randOperator1.setTuplesBlast(5000);
    randOperator2.setMinvalue(0);
    randOperator2.setMaxvalue(maxValue);
    randOperator2.setTuplesBlast(5000);
    sumOperator.setBase(maxValue * maxValue);
//    dag.getContextAttributes(randOperator1).attr(OperatorContext.INITIAL_PARTITION_COUNT).set(3);
//    dag.getContextAttributes(randOperator2).attr(OperatorContext.INITIAL_PARTITION_COUNT).set(3);
//    dag.getContextAttributes(sumOperator).attr(OperatorContext.INITIAL_PARTITION_COUNT).set(2);

    /* wire the operators together using streams */
    dag.addStream("rand1_sum", randOperator1.integer_data, sumOperator.input1).setInline(allInline);
    dag.addStream("rand2_sum", randOperator2.integer_data, sumOperator.input2).setInline(allInline);

    /* lets add more stuff to see what our dag is outputting */
//    ConsoleOutputOperator<Integer> consoleOperator = dag.addOperator("console", new ConsoleOutputOperator<Integer>());
//    dag.addStream("sum_console", sumOperator.output, consoleOperator.input).setInline(allInline);

    return dag;
  }

}