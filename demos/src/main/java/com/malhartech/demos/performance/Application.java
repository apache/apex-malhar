/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.performance;

import com.malhartech.dag.ApplicationFactory;
import com.malhartech.dag.Component;
import com.malhartech.dag.DAG;
import com.malhartech.dag.DAG.Operator;
import com.malhartech.demos.twitter.TwitterSampleInput;
import com.malhartech.demos.twitter.TwitterStatusURLExtractor;
import com.malhartech.lib.algo.WindowedTopCounter;
import com.malhartech.lib.io.ConsoleOutputModule;
import com.malhartech.lib.io.HttpOutputModule;
import com.malhartech.lib.math.UniqueCounter;
import java.io.IOException;
import java.util.Map.Entry;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    Operator noOpProcessor = b.addOperator("noOpProcessor", DoNothingModule.class);
    Operator counter = b.addOperator("counter", WordCountModule.class);

    b.addStream("Generator2Processor", wordGenerator.getOutput(Component.OUTPUT), noOpProcessor.getInput(Component.INPUT)).setInline(inline);
    b.addStream("Processor2Counter", noOpProcessor.getOutput(Component.OUTPUT), counter.getInput(Component.INPUT)).setInline(inline);

    return b;
  }
}
