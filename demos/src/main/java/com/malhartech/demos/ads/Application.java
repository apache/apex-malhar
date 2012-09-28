/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.ads;

import com.malhartech.dag.ApplicationFactory;
import com.malhartech.dag.DAG;
import com.malhartech.dag.DAG.Operator;
import com.malhartech.lib.io.ConsoleOutputModule;
import com.malhartech.lib.io.HttpOutputModule;
import com.malhartech.lib.math.ArithmeticMargin;
import com.malhartech.lib.math.ArithmeticQuotient;
import com.malhartech.lib.math.ArithmeticSum;
import com.malhartech.lib.testbench.FilterClassifier;
import com.malhartech.lib.testbench.LoadClassifier;
import com.malhartech.lib.testbench.LoadGenerator;


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

  public Operator getSumOperator(String name, DAG b) {
    return b.addOperator(name, ArithmeticSum.class);
  }

  public Operator getMarginOperator(String name, DAG b) {
    return b.addOperator(name, ArithmeticMargin.class).setProperty(ArithmeticMargin.KEY_PERCENT, "true");
  }

  public Operator getQuotientOperator(String name, DAG b) {
    Operator oper = b.addOperator(name, ArithmeticQuotient.class);
    oper.setProperty(ArithmeticQuotient.KEY_MULTIPLY_BY, "10");
    oper.setProperty(ArithmeticQuotient.KEY_DOKEY, "true");
    return oper;
  }

  public Operator getPageViewGenOperator(String name, DAG b) {
    Operator oper = b.addOperator(name, LoadGenerator.class);
    oper.setProperty(LoadGenerator.KEY_KEYS, "home,finance,sports,mail");
    oper.setProperty(LoadGenerator.KEY_VALUES, "0.00215,0.003,0.00175,0.0006"); // average value for each key, i.e. cost of getting an impression
    oper.setProperty(LoadGenerator.KEY_WEIGHTS, "25,25,25,25");
    oper.setProperty(LoadGenerator.KEY_TUPLES_BLAST, String.valueOf(this.generatorVTuplesBlast));
    oper.setProperty(LoadGenerator.MAX_WINDOWS_COUNT, String.valueOf(generatorMaxWindowsCount));
    oper.setProperty(LoadGenerator.ROLLING_WINDOW_COUNT, String.valueOf(this.generatorWindowCount));
    return oper;
  }

  public Operator getAdViewsStampOperator(String name, DAG b) {
    Operator oper = b.addOperator(name, LoadClassifier.class);
    oper.setProperty(LoadClassifier.KEY_KEYS, "sprint,etrade,nike");
    return oper;
  }

  public Operator getInsertClicksOperator(String name, DAG b) {
    return b.addOperator(name, FilterClassifier.class)
        .setProperty(FilterClassifier.KEY_KEYS, "sprint,etrade,nike")
        .setProperty(FilterClassifier.KEY_WEIGHTS, "home:60,10,30;finance:10,75,15;sports:10,10,80;mail:50,15,35") // ctr in ratio
        .setProperty(FilterClassifier.KEY_VALUES, "0.1,0.5,0.4")
        .setProperty(FilterClassifier.KEY_FILTER, "40,1000"); // average value for each classify_key, i.e. money paid by advertizer
  }

  @Override
  public DAG getApplication() {

    DAG dag = new DAG();

    Operator viewGen = getPageViewGenOperator("viewGen", dag);
    Operator adviews = getAdViewsStampOperator("adviews", dag);
    Operator insertclicks = getInsertClicksOperator("insertclicks", dag);
    Operator ctr = getQuotientOperator("ctr", dag);
    Operator cost = getSumOperator("cost", dag);
    Operator revenue = getSumOperator("rev", dag);
    Operator margin = getMarginOperator("margin", dag);

    dag.addStream("views", viewGen.getOutput(LoadGenerator.OPORT_DATA), adviews.getInput(LoadClassifier.IPORT_IN_DATA)).setInline(true);
    dag.addStream("adviewsdata", adviews.getOutput(LoadClassifier.OPORT_OUT_DATA), insertclicks.getInput(FilterClassifier.IPORT_IN_DATA),
                      ctr.getInput(ArithmeticQuotient.IPORT_DENOMINATOR), cost.getInput(ArithmeticSum.IPORT_DATA)).setInline(true);
    dag.addStream("clicksdata", insertclicks.getOutput(FilterClassifier.OPORT_OUT_DATA),
                      ctr.getInput(ArithmeticQuotient.IPORT_NUMERATOR), revenue.getInput(ArithmeticSum.IPORT_DATA)).setInline(true);

    Operator revconsole = getConsoleOperator(dag, "revConsole");
    Operator costconsole = getConsoleOperator(dag, "costConsole");
    Operator marginconsole = getConsoleOperator(dag, "marginConsole");
    Operator ctrconsole = getConsoleOperator(dag, "ctrConsole");
    Operator viewcountconsole = getConsoleOperator(dag, "viewCountConsole");

    dag.addStream("revenuedata", revenue.getOutput(ArithmeticSum.OPORT_SUM), margin.getInput(ArithmeticMargin.IPORT_DENOMINATOR), revconsole.getInput(ConsoleOutputModule.INPUT)).setInline(true);
    dag.addStream("costdata", cost.getOutput(ArithmeticSum.OPORT_SUM), margin.getInput(ArithmeticMargin.IPORT_NUMERATOR), costconsole.getInput(ConsoleOutputModule.INPUT)).setInline(true);
    dag.addStream("margindata", margin.getOutput(ArithmeticMargin.OPORT_MARGIN), marginconsole.getInput(ConsoleOutputModule.INPUT)).setInline(true);
    dag.addStream("ctrdata", ctr.getOutput(ArithmeticQuotient.OPORT_QUOTIENT), ctrconsole.getInput(ConsoleOutputModule.INPUT)).setInline(true);
    dag.addStream("tuplecount", viewGen.getOutput(LoadGenerator.OPORT_COUNT), viewcountconsole.getInput(ConsoleOutputModule.INPUT)).setInline(true);

    // these settings only affect distributed mode
    dag.getConf().setInt(DAG.STRAM_CONTAINER_MEMORY_MB, 2048);
    dag.getConf().setInt(DAG.STRAM_MASTER_MEMORY_MB, 1024);

    return dag;
  }
}

