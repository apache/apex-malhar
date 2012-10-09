/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.performance;

import com.malhartech.dag.ApplicationFactory;
import com.malhartech.dag.Component;
import com.malhartech.dag.DAG;
import com.malhartech.dag.DAG.Operator;
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
    DAG b = new DAG(conf);

    Operator wordGenerator = b.addOperator("wordGenerator", RandomWordInputModule.class);
//    Operator noOpProcessor = b.addOperator("noOpProcessor", DoNothingModule.class);
    Operator counter = b.addOperator("counter", WordCountModule.class);

//    b.addStream("Generator2Processor", wordGenerator.getOutput(Component.OUTPUT), noOpProcessor.getInput(Component.INPUT)).setInline(inline);
//    b.addStream("Processor2Counter", noOpProcessor.getOutput(Component.OUTPUT), counter.getInput(Component.INPUT)).setInline(inline);

    b.addStream("Generator2Counter", wordGenerator.getOutput(Component.OUTPUT), counter.getInput(Component.INPUT)).setInline(false);
    return b;
  }
}
