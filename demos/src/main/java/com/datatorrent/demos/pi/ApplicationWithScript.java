/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.pi;

import com.datatorrent.api.ApplicationFactory;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.math.Script;
import com.datatorrent.lib.stream.RoundRobinHashMap;
import com.datatorrent.lib.testbench.RandomEventGenerator;

import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class ApplicationWithScript implements ApplicationFactory
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    int maxValue = 30000;

    RandomEventGenerator rand = dag.addOperator("rand", new RandomEventGenerator());
    rand.setMinvalue(0);
    rand.setMaxvalue(maxValue);

    RoundRobinHashMap<String,Object> rrhm = dag.addOperator("rrhm", new RoundRobinHashMap<String, Object>());
    rrhm.setKeys(new String[] { "x", "y" });

    Script calc = dag.addOperator("picalc", new Script());
    calc.setKeepContext(true);
    calc.setPassThru(false);
    calc.put("i",0);
    calc.put("count",0);
    calc.addSetupScript("function pi() { if (x*x+y*y <= "+maxValue*maxValue+") { i++; } count++; return i / count * 4; }");

    calc.setInvoke("pi");

    dag.addStream("rand_rrhm", rand.integer_data, rrhm.data);
    dag.addStream("rrhm_calc", rrhm.map, calc.inBindings);

    ConsoleOutputOperator console = dag.addOperator("console", new ConsoleOutputOperator());
    dag.addStream("rand_console",calc.result, console.input);

  }

}
