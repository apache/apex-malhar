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
  public void getApplication(DAG dag, Configuration conf)
  {
    RandomWordInputModule wordGenerator = dag.addOperator("wordGenerator", RandomWordInputModule.class);
    dag.getMeta(wordGenerator).getMeta(wordGenerator.output).getAttributes().attr(PortContext.QUEUE_CAPACITY).set(32 * 1024);

    WordCountOperator<byte[]> counter = dag.addOperator("counter", new WordCountOperator<byte[]>());
    dag.getMeta(counter).getMeta(counter.input).getAttributes().attr(PortContext.QUEUE_CAPACITY).set(32 * 1024);

    dag.addStream("Generator2Counter", wordGenerator.output, counter.input).setInline(inline);
  }

}
