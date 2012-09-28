/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.mobile;

import com.malhartech.dag.ApplicationFactory;
import com.malhartech.dag.DAG;
import com.malhartech.dag.DAG.Operator;
import com.malhartech.lib.io.ConsoleOutputModule;
import com.malhartech.lib.io.HttpOutputModule;
import com.malhartech.lib.testbench.LoadSeedGenerator;


/**
 * Example of application configuration in Java using {@link com.malhartech.stram.conf.NewDAGBuilder}.<p>
 */

public class Application implements ApplicationFactory {

  // adjust these depending on execution mode (junit, cli-local, cluster)
  private int generatorVTuplesBlast = 1000;
  private int generatorMaxWindowsCount = 100;
  private int generatorWindowCount = 1;

  {
    // TODO: call from the CLI
    setLocalMode();
  }

  public void setUnitTestMode() {
   generatorVTuplesBlast = 10;
   generatorWindowCount = 5;
   generatorMaxWindowsCount = 20;
  }

  public void setLocalMode() {
    generatorVTuplesBlast = 5000; // keep this number low to not distort window boundaries
    //generatorVTuplesBlast = 500000;
   generatorWindowCount = 5;
   //generatorMaxWindowsCount = 50;
   generatorMaxWindowsCount = 1000;
  }

  private Operator getConsoleOperator(DAG b, String operatorName)
  {
    // output to HTTP server when specified in environment setting
    String serverAddr = System.getenv("MALHAR_AJAXSERVER_ADDRESS");
    if (serverAddr != null) {
      return b.addOperator(operatorName, HttpOutputModule.class)
              .setProperty(HttpOutputModule.P_RESOURCE_URL, "http://" + serverAddr + "/channel/" + operatorName);
    }
    return b.addOperator(operatorName, ConsoleOutputModule.class)
            //.setProperty(ConsoleOutputModule.P_DEBUG, "true")
            .setProperty(ConsoleOutputModule.P_STRING_FORMAT, operatorName + ": %s");
  }

  public Operator getSeedGeneratorOperator(String name, DAG b) {
    Operator oper = b.addOperator(name, LoadSeedGenerator.class);
    oper.setProperty(LoadSeedGenerator.KEY_STRING_SCHEMA, "false");
    oper.setProperty(LoadSeedGenerator.KEY_EMITKEY, "false");
    oper.setProperty(LoadSeedGenerator.KEY_KEYS, "x,y");

    return oper;
  }


  @Override
  public DAG getApplication() {

    DAG dag = new DAG();


    Operator seedGen = getSeedGeneratorOperator("seedGen", dag);

    //dag.addStream("views", viewGen.getOutput(LoadGenerator.OPORT_DATA), adviews.getInput(LoadClassifier.IPORT_IN_DATA)).setInline(true);

    Operator viewcountconsole = getConsoleOperator(dag, "viewCountConsole");

    //dag.addStream("revenuedata", revenue.getOutput(ArithmeticSum.OPORT_SUM), margin.getInput(ArithmeticMargin.IPORT_DENOMINATOR), revconsole.getInput(ConsoleOutputModule.INPUT)).setInline(true);

    // these settings only affect distributed mode
    dag.getConf().setInt(DAG.STRAM_CONTAINER_MEMORY_MB, 512);
    dag.getConf().setInt(DAG.STRAM_MASTER_MEMORY_MB, 512);

    return dag;
  }
}

