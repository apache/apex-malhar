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
import com.malhartech.lib.testbench.StreamMerger;
import com.malhartech.lib.testbench.StreamMerger10;
import com.malhartech.lib.testbench.StreamMerger5;
import com.malhartech.lib.testbench.ThroughputCounter;
import org.apache.hadoop.conf.Configuration;


/**
 * Example of application configuration in Java using {@link com.malhartech.stram.conf.NewDAGBuilder}.<p>
 */

public class Application5 implements ApplicationFactory {

  public static final String P_numGenerators = Application5.class.getName() + ".numGenerators";
  public static final String P_generatorVTuplesBlast = Application5.class.getName() + ".generatorVTuplesBlast";
  public static final String P_generatorMaxWindowsCount = Application5.class.getName() + ".generatorMaxWindowsCount";
  public static final String P_allInline = Application5.class.getName() + ".allInline";

  // adjust these depending on execution mode (junit, cli-local, cluster)
  private int generatorVTuplesBlast = 1000;
  private int generatorMaxWindowsCount = 100;
  private int generatorWindowCount = 1;
  private int numGenerators = 2;
  private boolean allInline = true;

  public void setUnitTestMode() {
   generatorVTuplesBlast = 10;
   generatorWindowCount = 5;
   generatorMaxWindowsCount = 20;
  }

  public void setLocalMode() {
    generatorVTuplesBlast = 1000; // keep this number low to not distort window boundaries
    //generatorVTuplesBlast = 500000;
   generatorWindowCount = 5;
   //generatorMaxWindowsCount = 50;
   generatorMaxWindowsCount = 1000;
  }

  private void configure(Configuration conf) {

    if (LAUNCHMODE_YARN.equals(conf.get(DAG.STRAM_LAUNCH_MODE))) {
      setLocalMode();
      // settings only affect distributed mode
      conf.setIfUnset(DAG.STRAM_CONTAINER_MEMORY_MB, "2048");
      conf.setIfUnset(DAG.STRAM_MASTER_MEMORY_MB, "1024");
      conf.setIfUnset(DAG.STRAM_MAX_CONTAINERS, "1");
    } else if (LAUNCHMODE_LOCAL.equals(conf.get(DAG.STRAM_LAUNCH_MODE))) {
      setLocalMode();
    }

    this.numGenerators = conf.getInt(P_numGenerators, this.numGenerators);
    this.generatorVTuplesBlast = conf.getInt(P_generatorVTuplesBlast, this.generatorVTuplesBlast);
    this.generatorMaxWindowsCount = conf.getInt(P_generatorMaxWindowsCount, this.generatorMaxWindowsCount);
    this.allInline = conf.getBoolean(P_allInline, this.allInline);

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

  public Operator getThroughputCounter(String name, DAG b) {
    return b.addOperator(name, ThroughputCounter.class).setProperty(ThroughputCounter.ROLLING_WINDOW_COUNT, "5");
  }

  public Operator getMarginOperator(String name, DAG b) {
    return b.addOperator(name, ArithmeticMargin.class).setProperty(ArithmeticMargin.KEY_PERCENT, "true");
  }


  public Operator getQuotientOperator(String name, DAG b) {
    Operator oper = b.addOperator(name, ArithmeticQuotient.class);
    oper.setProperty(ArithmeticQuotient.KEY_MULTIPLY_BY, "100"); // multiply by 100 to get percentage
    oper.setProperty(ArithmeticQuotient.KEY_DOKEY, "true"); // Ignore the value and just count instances or a key
    oper.setProperty("do_debug", "CTR Node");
    return oper;
  }

  public Operator getPageViewGenOperator(String name, DAG b) {
    Operator oper = b.addOperator(name, LoadGenerator.class);
    oper.setProperty(LoadGenerator.KEY_KEYS, "home,finance,sports,mail");
    // Paying $2.15,$3,$1.75,$.6 for 1000 views respectively
    oper.setProperty(LoadGenerator.KEY_VALUES, "0.00215,0.003,0.00175,0.0006");
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
    Operator oper = b.addOperator(name, FilterClassifier.class);
    oper.setProperty(FilterClassifier.KEY_KEYS, "sprint,etrade,nike");
    oper.setProperty(FilterClassifier.KEY_WEIGHTS, "home:60,10,30;finance:10,75,15;sports:10,10,80;mail:50,15,35"); // ctr in ratio
    // Getting $1,$5,$4 per click respectively
    oper.setProperty(FilterClassifier.KEY_VALUES, "1,5,4");
    oper.setProperty(FilterClassifier.KEY_FILTER, "40,1000"); // average value for each classify_key, i.e. money paid by advertizer
    return oper;
  }

  public Operator getStreamMerger10Operator(String name, DAG b) {
    Operator oper = b.addOperator(name, StreamMerger10.class);
    return oper;
  }

  public Operator getStreamMerger5Operator(String name, DAG b) {
    Operator oper = b.addOperator(name, StreamMerger5.class);
    return oper;
  }

  public Operator getStreamMerger(String name, DAG b) {
    return b.addOperator(name, StreamMerger.class);
  }

  @Override
  public DAG getApplication(Configuration conf) {

    configure(conf);

    DAG dag = new DAG(conf);

    Operator viewAggrSum5 = getStreamMerger10Operator("viewaggregatesum", dag);
    Operator clickAggrSum5 = getStreamMerger10Operator("clickaggregatesum", dag);
    Operator viewAggrCount5 = getStreamMerger10Operator("viewaggregatecount", dag);
    Operator clickAggrCount5 = getStreamMerger10Operator("clickaggregatecount", dag);

    for (int i = 1; i <= numGenerators; i++) {
      String viewgenstr = String.format("%s%d", "viewGen", i);
      String adviewstr = String.format("%s%d", "adviews", i);
      String insertclicksstr = String.format("%s%d", "insertclicks", i);
      String viewAggrstr = String.format("%s%d", "viewAggr", i);
      String clickAggrstr = String.format("%s%d", "clickAggr", i);

      String viewsstreamstr = String.format("%s%d", "views", i);
      String viewsaggregatesrteamstr = String.format("%s%d", "viewsaggregate", i);
      String clicksaggregatestreamstr = String.format("%s%d", "clicksaggregate", i);
      String viewaggrsumstreamstr = String.format("%s%d", "viewsaggrsum", i);
      String clickaggrsumstreamstr = String.format("%s%d", "clicksaggrsum", i);
      String viewaggrcountstreamstr = String.format("%s%d", "viewsaggrcount", i);
      String clickaggrcountstreamstr = String.format("%s%d", "clicksaggrcount", i);

      Operator viewGen = getPageViewGenOperator(viewgenstr, dag);
      Operator adviews = getAdViewsStampOperator(adviewstr, dag);
      Operator insertclicks = getInsertClicksOperator(insertclicksstr, dag);
      Operator viewAggregate = getSumOperator(viewAggrstr, dag);
      Operator clickAggregate = getSumOperator(clickAggrstr, dag);

      dag.addStream(viewsstreamstr, viewGen.getOutput(LoadGenerator.OPORT_DATA), adviews.getInput(LoadClassifier.IPORT_IN_DATA)).setInline(true);
      dag.addStream(viewsaggregatesrteamstr, adviews.getOutput(LoadClassifier.OPORT_OUT_DATA), insertclicks.getInput(FilterClassifier.IPORT_IN_DATA),
                    viewAggregate.getInput(ArithmeticSum.IPORT_DATA)).setInline(true);
      dag.addStream(clicksaggregatestreamstr, insertclicks.getOutput(FilterClassifier.OPORT_OUT_DATA), clickAggregate.getInput(ArithmeticSum.IPORT_DATA)).setInline(true);

      dag.addStream(viewaggrsumstreamstr, viewAggregate.getOutput(ArithmeticSum.OPORT_SUM), viewAggrSum5.getInput(StreamMerger10.getInputName(i)));
      dag.addStream(clickaggrsumstreamstr, clickAggregate.getOutput(ArithmeticSum.OPORT_SUM), clickAggrSum5.getInput(StreamMerger10.getInputName(i)));
      dag.addStream(viewaggrcountstreamstr, viewAggregate.getOutput(ArithmeticSum.OPORT_COUNT), viewAggrCount5.getInput(StreamMerger10.getInputName(i)));
      dag.addStream(clickaggrcountstreamstr, clickAggregate.getOutput(ArithmeticSum.OPORT_COUNT), clickAggrCount5.getInput(StreamMerger10.getInputName(i)));
    }

    Operator ctr = getQuotientOperator("ctr", dag);
    Operator cost = getSumOperator("cost", dag);
    Operator revenue = getSumOperator("rev", dag);
    Operator margin = getMarginOperator("margin", dag);
    Operator merge = getStreamMerger("countmerge", dag);
    Operator tuple_counter = getThroughputCounter("tuple_counter", dag);

    dag.addStream("adviewsdata", viewAggrSum5.getOutput(StreamMerger5.OPORT_OUT_DATA), cost.getInput(ArithmeticSum.IPORT_DATA));
    dag.addStream("clicksdata", clickAggrSum5.getOutput(StreamMerger5.OPORT_OUT_DATA), revenue.getInput(ArithmeticSum.IPORT_DATA));
    dag.addStream("viewtuplecount", viewAggrCount5.getOutput(StreamMerger5.OPORT_OUT_DATA), ctr.getInput(ArithmeticQuotient.IPORT_DENOMINATOR)
            , merge.getInput(StreamMerger.IPORT_IN_DATA1));
    dag.addStream("clicktuplecount", clickAggrCount5.getOutput(StreamMerger5.OPORT_OUT_DATA), ctr.getInput(ArithmeticQuotient.IPORT_NUMERATOR)
            , merge.getInput(StreamMerger.IPORT_IN_DATA2));
    dag.addStream("total count", merge.getOutput(StreamMerger.OPORT_OUT_DATA), tuple_counter.getInput(ThroughputCounter.IPORT_DATA));


    Operator revconsole = getConsoleOperator(dag, "revConsole");
    Operator costconsole = getConsoleOperator(dag, "costConsole");
    Operator marginconsole = getConsoleOperator(dag, "marginConsole");
    Operator ctrconsole = getConsoleOperator(dag, "ctrConsole");
    Operator viewcountconsole = getConsoleOperator(dag, "viewCountConsole");

    dag.addStream("revenuedata", revenue.getOutput(ArithmeticSum.OPORT_SUM), margin.getInput(ArithmeticMargin.IPORT_DENOMINATOR), revconsole.getInput(ConsoleOutputModule.INPUT)).setInline(allInline);
    dag.addStream("costdata", cost.getOutput(ArithmeticSum.OPORT_SUM), margin.getInput(ArithmeticMargin.IPORT_NUMERATOR), costconsole.getInput(ConsoleOutputModule.INPUT)).setInline(allInline);
    dag.addStream("margindata", margin.getOutput(ArithmeticMargin.OPORT_MARGIN), marginconsole.getInput(ConsoleOutputModule.INPUT)).setInline(allInline);
    dag.addStream("ctrdata", ctr.getOutput(ArithmeticQuotient.OPORT_QUOTIENT), ctrconsole.getInput(ConsoleOutputModule.INPUT)).setInline(allInline);
    dag.addStream("tuplecount", tuple_counter.getOutput(ThroughputCounter.OPORT_COUNT) , viewcountconsole.getInput(ConsoleOutputModule.INPUT)).setInline(allInline);

    return dag;
  }
}

