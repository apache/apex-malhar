/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.pi;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.lib.testbench.RandomEventGenerator;
import org.apache.hadoop.conf.Configuration;

/**
 * This is an an example of DAG which can be used to calculate value of PI.
 *
 * This demo demonstrates the calculation of PI.
 * Using two operators to generate two random numbers each as x and y in the range of [0,r].
 * If x^2+y^2 less equal than r^2, it means it is inside the circle range. If not, it is outside the circle range.
 * The number of inside divided by the number of outside is constant (Pi/4)
 *
 * @author Zhongjian Wang<zhongjian@malhar-inc.com>
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
    RandomEventGenerator randOperator1 = dag.addOperator("rand1", new RandomEventGenerator());
    RandomEventGenerator randOperator2 = dag.addOperator("rand2", new RandomEventGenerator());
    SumCompareOperator sumOperator = dag.addOperator("sum", new SumCompareOperator());
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

    dag.addStream("rand1_sum", randOperator1.integer_data, sumOperator.input1).setInline(allInline);
    dag.addStream("rand2_sum", randOperator2.integer_data, sumOperator.input2).setInline(allInline);

//    ConsoleOutputOperator<Integer> consoleOperator = dag.addOperator("console", new ConsoleOutputOperator<Integer>());
//    dag.addStream("sum_console", sumOperator.output, consoleOperator.input).setInline(allInline);

    return dag;
  }
}