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
public class ApplicationFixed implements ApplicationFactory
{
  private static final boolean inline = false;
  public static final int QUEUE_CAPACITY = 16 * 1024;

  @Override
  public DAG getApplication(Configuration conf)
  {
    DAG dag = new DAG(conf);

    FixedTuplesInputOperator wordGenerator = dag.addOperator("WordGenerator", FixedTuplesInputOperator.class);
    dag.getOperatorMeta(wordGenerator).getOutputPortMeta(wordGenerator.output).getAttributes().attr(PortContext.QUEUE_CAPACITY).set(QUEUE_CAPACITY);
    wordGenerator.setCount(500000);

    WordCountOperator<byte[]> counter = dag.addOperator("Counter", new WordCountOperator<byte[]>());
    dag.getOperatorMeta(counter).getInputPortMeta(counter.input).getAttributes().attr(PortContext.QUEUE_CAPACITY).set(QUEUE_CAPACITY);

    dag.addStream("Generator2Counter", wordGenerator.output, counter.input).setInline(inline);
    return dag;
  }
}
