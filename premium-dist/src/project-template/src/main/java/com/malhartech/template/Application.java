/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.template;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.testbench.SeedEventGenerator;

public class Application implements ApplicationFactory
{

  @Override
  public DAG getApplication(Configuration conf)
  {
    // Sample DAG with 2 operators

    DAG dag = new DAG(conf);

    SeedEventGenerator seedGen = dag.addOperator("seedGen", SeedEventGenerator.class);
    seedGen.setSeedstart(1);
    seedGen.setSeedend(2);
    seedGen.addKeyData("x", 0, 10);
    seedGen.addKeyData("y", 0, 100);

    ConsoleOutputOperator cons = dag.addOperator("console", new ConsoleOutputOperator());
    cons.setStringFormat("hello: %s");

    dag.addStream("seeddata", seedGen.val_list, cons.input).setInline(true);

    return dag;
  }
}
