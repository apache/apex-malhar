/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.pi2;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.math.Script;
import com.malhartech.lib.stream.RoundRobinHashMap;
import com.malhartech.lib.testbench.RandomEventGenerator;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.script.ScriptException;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author David Yan <davidyan@malhar-inc.com>
 */
public class Application implements ApplicationFactory
{

  @Override
  public DAG getApplication(Configuration conf)
  {
    DAG dag = new DAG(conf);
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

    return dag;
  }

}
