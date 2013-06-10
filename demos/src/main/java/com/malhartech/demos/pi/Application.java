/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.pi;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.testbench.RandomEventGenerator;

import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class Application implements ApplicationFactory
{
  private final boolean allInline = false;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    int maxValue = 30000;

    RandomEventGenerator rand = dag.addOperator("rand", new RandomEventGenerator());
    rand.setMinvalue(0);
    rand.setMaxvalue(maxValue);

    PiCalculateOperator calc = dag.addOperator("picalc", new PiCalculateOperator());
    calc.setBase(maxValue*maxValue);
    dag.addStream("rand_calc", rand.integer_data, calc.input).setInline(allInline);

    ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("rand_console",calc.output, console.input).setInline(allInline);

  }

}
