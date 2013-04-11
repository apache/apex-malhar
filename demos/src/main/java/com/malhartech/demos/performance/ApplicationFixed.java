/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.performance;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import org.apache.hadoop.conf.Configuration;

/**
 * Example of application configuration in Java.<p>
 */
public class ApplicationFixed implements ApplicationFactory
{
  private static final boolean inline = false;

  @Override
  public DAG getApplication(Configuration conf)
  {
    DAG dag = new DAG(conf);

    FixedTuplesInputModule wordGenerator = dag.addOperator("WordGenerator", FixedTuplesInputModule.class);
    wordGenerator.setCount(250000);
    WordCountModule<byte[]> counter = dag.addOperator("Counter", new WordCountModule<byte[]>());
    dag.addStream("Generator2Counter", wordGenerator.output, counter.input).setInline(inline);
    return dag;
  }
}
