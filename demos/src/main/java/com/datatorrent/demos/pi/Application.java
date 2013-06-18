/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.pi;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.testbench.RandomEventGenerator;

import org.apache.hadoop.conf.Configuration;

/**
 * Monte Carlo PI extimation demo : <br>
 * This application computes value of PI using Monte Carlo pi estimation
 * formula.
 * 
 * Run Sample Application : <br>
 * Please consult Application Developer guide <a href=
 * "https://docs.google.com/document/d/1WX-HbsGQPV_DfM1tEkvLm1zD_FLYfdLZ1ivVHqzUKvE/edit#heading=h.lfl6f68sq80m"
 * > here </a>.
 * <p>
 * Running Java Test or Main app in IDE:
 * 
 * <pre>
 * LocalMode.runApp(new Application(), 600000); // 10 min run
 * </pre>
 * 
 * Run Success : <br>
 * For successful deployment and run, user should see following output on
 * console:
 * 
 * <pre>
 * 3.1430480549199085
 * 3.1423454157782515
 * 3.1431377245508982
 * 3.142078799249531
 * 2013-06-18 10:43:18,335 [main] INFO  stram.StramLocalCluster run - Application finished.
 * </pre>
 * 
 * Application DAG : <br>
 * <img src="doc-files/Application.gif" width=600px > <br>
 * <br>
 * 
 * Streaming Window Size : 1000 ms(1 Sec) <br>
 * Operator Details : <br>
 * <ul>
 * <li><b>The rand Operator : </b> This operator generates random integer
 * between 0-30k. <br>
 * Class : com.datatorrent.lib.testbench.RandomEventGenerator <br>
 * State Less : Yes</li>
 * <li><b>The calc operator : </b> This operator computes value of pi using
 * monte carlo estimation. <br>
 * Class : com.datatorrent.demos.pi.PiCalculateOperator <br>
 * State Less : Yes</li>
 * <li><b>The operator Console: </b> This operator just outputs the input tuples
 * to the console (or stdout). <br>
 * if you need to change write to HDFS,HTTP .. instead of console, <br>
 * Please refer to {@link com.datatorrent.lib.io.HttpOutputOperator} or
 * {@link com.datatorrent.lib.io.HdfsOutputOperator}.</li>
 * </ul>
 * 
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class Application implements StreamingApplication
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
