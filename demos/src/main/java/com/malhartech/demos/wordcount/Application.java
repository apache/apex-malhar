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
 *
 * This program will count the number of words.
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

    RandomSentenceInputOperator input = dag.addOperator("sentence", new RandomSentenceInputOperator());
    WordCountOutputOperator wordCount = dag.addOperator("count", new WordCountOutputOperator());

    dag.addStream("sentence-count", input.output, wordCount.input).setInline(allInline);

    ConsoleOutputOperator consoleOperator = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("count-console",wordCount.output, consoleOperator.input);


    return dag;
  }


}
