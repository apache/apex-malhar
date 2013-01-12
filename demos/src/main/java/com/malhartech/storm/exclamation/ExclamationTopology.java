/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.storm.exclamation;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DAG;
import com.malhartech.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;

/**
 * This is an imitation of ExclamationTopology example of storm using Malhar model.
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class ExclamationTopology implements ApplicationFactory
{
  private final boolean allInline = true;

  @Override
  public DAG getApplication(Configuration conf)
  {
    /* dag is equivalent to topology so we build it directly */
    DAG dag = new DAG(conf);
    dag.getAttributes().attr(DAG.STRAM_MAX_CONTAINERS).set(1); /* inline is ignored with partitioning */

    /* add all the input operators (spouts) and operators (bolts) to the dag */
    TestWordSpout wordOperator = dag.addOperator("word", new TestWordSpout());
    ExclamationBolt exclaim1Operator = dag.addOperator("exclaim1", new ExclamationBolt());
    ExclamationBolt exclaim2Operator = dag.addOperator("exclaim2", new ExclamationBolt());

    //dag.getContextAttributes(wordOperator).attr(OperatorContext.INITIAL_PARTITION_COUNT).set(10);
    dag.getContextAttributes(exclaim1Operator).attr(OperatorContext.INITIAL_PARTITION_COUNT).set(3);
    //dag.getContextAttributes(exclaim2Operator).attr(OperatorContext.INITIAL_PARTITION_COUNT).set(2);

    /* wire the operators together using streams */
    dag.addStream("word_exclaim", wordOperator.output, exclaim1Operator.input).setInline(allInline);
    dag.addStream("exclaim1_exclaim2", exclaim1Operator.output, exclaim2Operator.input).setInline(allInline);

    /* lets add more stuff to see what our dag is outputting */
    ConsoleOutputOperator<String> consoleOperator = dag.addOperator("console", new ConsoleOutputOperator<String>());
    dag.addStream("exclaim2_console", exclaim2Operator.output, consoleOperator.input).setInline(allInline);

    return dag;
  }

}
