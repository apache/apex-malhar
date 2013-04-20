/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.performance;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.Context.PortContext;
import com.malhartech.api.DAG;
import org.apache.hadoop.conf.Configuration;

/**
 * Example of application configuration in Java.<p>
 */
public class Application implements ApplicationFactory
{
  private static final boolean inline = false;

  @Override
  public DAG getApplication(Configuration conf)
  {
    DAG dag = new DAG(conf);

    RandomWordInputModule wordGenerator = dag.addOperator("wordGenerator", RandomWordInputModule.class);
//    DoNothingModule<byte[]> noOpProcessor = dag.addOperator("noOpProcessor", new DoNothingModule<byte[]>());
    WordCountModule<byte[]> counter = dag.addOperator("counter", new WordCountModule<byte[]>());

//    dag.addStream("Generator2Processor", wordGenerator.output, noOpProcessor.input).setInline(inline);
//    dag.addStream("Processor2Counter", noOpProcessor.output, counter.input).setInline(inline);
    dag.getOperatorMeta(counter).getInputPortMeta(counter.input).getAttributes().attr(PortContext.QUEUE_CAPACITY).set(16 * 1024);

    dag.addStream("Generator2Counter", wordGenerator.output, counter.input).setInline(inline);
    return dag;
  }

}
