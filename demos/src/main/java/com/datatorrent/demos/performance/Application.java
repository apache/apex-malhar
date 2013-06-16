/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.datatorrent.demos.performance;

import com.datatorrent.api.ApplicationFactory;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Context.PortContext;

import org.apache.hadoop.conf.Configuration;

/**
 * Example of application configuration in Java.<p>
 */
public class Application implements ApplicationFactory
{
  private static final boolean inline = false;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    RandomWordInputModule wordGenerator = dag.addOperator("wordGenerator", RandomWordInputModule.class);
    dag.getMeta(wordGenerator).getMeta(wordGenerator.output).getAttributes().attr(PortContext.QUEUE_CAPACITY).set(32 * 1024);

    WordCountOperator<byte[]> counter = dag.addOperator("counter", new WordCountOperator<byte[]>());
    dag.getMeta(counter).getMeta(counter.input).getAttributes().attr(PortContext.QUEUE_CAPACITY).set(32 * 1024);

    dag.addStream("Generator2Counter", wordGenerator.output, counter.input).setInline(inline);
  }

}
