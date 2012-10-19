/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.performance;

import com.malhartech.api.DAG;
import com.malhartech.dag.ApplicationFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * Example of application configuration in Java.<p>
 */
public class Application implements ApplicationFactory
{
  private static final boolean inline = true;

  @Override
  public DAG getApplication(Configuration conf)
  {
    DAG dag = new DAG(conf);
    dag.getConf().setInt(DAG.STRAM_CHECKPOINT_INTERVAL_MILLIS, 0); // disable auto backup

    RandomWordInputModule wordGenerator = dag.addOperator("wordGenerator", RandomWordInputModule.class);
//    DoNothingModule<byte[]> noOpProcessor = dag.addOperator("noOpProcessor", new DoNothingModule<byte[]>());
    WordCountModule<byte[]> counter = dag.addOperator("counter", WordCountModule.class);

//    dag.addStream("Generator2Processor", wordGenerator.output, noOpProcessor.input).setInline(inline);
//    dag.addStream("Processor2Counter", noOpProcessor.output, counter.input).setInline(inline);

    dag.addStream("Generator2Counter", wordGenerator.output, counter.input).setInline(inline);
    return dag;
  }
}
