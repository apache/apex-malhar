/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.wordcount;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.lib.io.ConsoleOutputOperator;
import org.apache.hadoop.conf.Configuration;

/**
 * To run the test, you need to generate a data file as sample file.
 * You can use "hadoop jar hadoop-*-examples.jar teragen 10000000000 in-dir" command to generate the data file. The number here is the line number of the file.
 * The "samplefile" provided contains 10000 files
 * The program will count the number of words of consecutive letters as "SSSSSSSSSS","HHHHHHHH" etc.
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class Application implements ApplicationFactory
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

    ConsoleOutputOperator consoleOperator = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("count-console",wordCount.output, consoleOperator.input);


    return dag;
  }


}
