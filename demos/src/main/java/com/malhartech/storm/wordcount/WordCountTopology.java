/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.storm.wordcount;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DAG;
import com.malhartech.lib.io.ConsoleOutputOperator;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class WordCountTopology implements ApplicationFactory
{
  private boolean allInline =  false;

  @Override
  public DAG getApplication(Configuration conf)
  {
    allInline = true;
    DAG dag =new DAG(conf);

    RandomSentenceSpout spout = dag.addOperator("spout", new RandomSentenceSpout());
    SplitSentence split = dag.addOperator("split", new SplitSentence());
    WordCount wordCount = dag.addOperator("count", new WordCount());

//    dag.getContextAttributes(spout).attr(OperatorContext.INITIAL_PARTITION_COUNT).set(4);
//    dag.getContextAttributes(split).attr(OperatorContext.INITIAL_PARTITION_COUNT).set(2);
//    dag.getContextAttributes(wordCount).attr(OperatorContext.INITIAL_PARTITION_COUNT).set(2);

    dag.addStream("spout-split",spout.output, split.input).setInline(allInline);
    dag.addStream("split-count", split.output, wordCount.input).setInline(allInline);

//    ConsoleOutputOperator<ArrayList<Object>> consoleOperator = dag.addOperator("console", new ConsoleOutputOperator<ArrayList<Object>>());
//    dag.addStream("count-console",wordCount.output, consoleOperator.input);


    return dag;
  }


}
