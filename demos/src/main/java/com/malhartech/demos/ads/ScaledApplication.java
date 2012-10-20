/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.ads;

import com.malhartech.dag.ApplicationFactory;
import com.malhartech.dag.DAG;
import com.malhartech.dag.DAG.OperatorInstance;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.io.HttpOutputOperator;
import com.malhartech.lib.math.Margin;
import com.malhartech.lib.math.Quotient;
import com.malhartech.lib.math.Sum;
import com.malhartech.lib.stream.StreamMerger;
import com.malhartech.lib.stream.StreamMerger10;
import com.malhartech.lib.testbench.FilteredEventClassifier;
import com.malhartech.lib.testbench.EventClassifier;
import com.malhartech.lib.testbench.EventGenerator;
import com.malhartech.lib.testbench.ThroughputCounter;
import org.apache.hadoop.conf.Configuration;


/**
 * Example of application configuration in Java using {@link com.malhartech.stram.conf.NewDAGBuilder}.<p>
 */

public class ScaledApplication implements ApplicationFactory {

  public static final String P_numGenerators = ScaledApplication.class.getName() + ".numGenerators";
  public static final String P_generatorVTuplesBlast = ScaledApplication.class.getName() + ".generatorVTuplesBlast";
  public static final String P_generatorMaxWindowsCount = ScaledApplication.class.getName() + ".generatorMaxWindowsCount";
  public static final String P_allInline = ScaledApplication.class.getName() + ".allInline";

  // adjust these depending on execution mode (junit, cli-local, cluster)
  private int generatorVTuplesBlast = 1000;
  private int generatorMaxWindowsCount = 100;
  private int generatorWindowCount = 1;
  private int numGenerators = 2;
  private boolean allInline = true;

  public void setUnitTestMode() {
   generatorVTuplesBlast = 10;
   generatorWindowCount = 25;
   generatorMaxWindowsCount = 20;
  }

  public void setLocalMode() {
    generatorVTuplesBlast = 1000; // keep this number low to not distort window boundaries
    generatorWindowCount = 25;
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

  private OperatorInstance getConsoleOperator(DAG b, String operatorName)
  {
    // output to HTTP server when specified in environment setting
    String serverAddr = System.getenv("MALHAR_AJAXSERVER_ADDRESS");
    if (serverAddr != null) {
      return b.addOperator(operatorName, HttpOutputOperator.class)
              .setProperty(HttpOutputOperator.P_RESOURCE_URL, "http://" + serverAddr + "/channel/" + operatorName);
    }
    return b.addOperator(operatorName, ConsoleOutputOperator.class)
            //.setProperty(ConsoleOutputOperator.P_DEBUG, "true")
            .setProperty(ConsoleOutputOperator.P_STRING_FORMAT, operatorName + ": %s");
  }

  public OperatorInstance getSumOperator(String name, DAG b) {
   return b.addOperator(name, Sum.class);
  }

  public OperatorInstance getThroughputCounter(String name, DAG b) {
    return b.addOperator(name, ThroughputCounter.class).
            setProperty(ThroughputCounter.ROLLING_WINDOW_COUNT, String.valueOf(this.generatorWindowCount));
  }

  public OperatorInstance getMarginOperator(String name, DAG b) {
    return b.addOperator(name, Margin.class).setProperty(Margin.KEY_PERCENT, "true");
  }


  public OperatorInstance getQuotientOperator(String name, DAG b) {
    OperatorInstance oper = b.addOperator(name, Quotient.class);
    oper.setProperty(Quotient.KEY_MULTIPLY_BY, "100"); // multiply by 100 to get percentage
    oper.setProperty(Quotient.KEY_DOKEY, "true"); // Ignore the value and just count instances or a key
    oper.setProperty("do_debug", "CTR Node");
    return oper;
  }

  public OperatorInstance getPageViewGenOperator(String name, DAG b) {
    OperatorInstance oper = b.addOperator(name, LoadGenerator.class);
    oper.setProperty(LoadGenerator.KEY_KEYS, "home,finance,sports,mail");
    // Paying $2.15,$3,$1.75,$.6 for 1000 views respectively
    oper.setProperty(EventGenerator.KEY_VALUES, "0.00215,0.003,0.00175,0.0006");
    oper.setProperty(EventGenerator.KEY_WEIGHTS, "25,25,25,25");
    oper.setProperty(EventGenerator.KEY_TUPLES_BLAST, String.valueOf(this.generatorVTuplesBlast));
    oper.setProperty(EventGenerator.MAX_WINDOWS_COUNT, String.valueOf(generatorMaxWindowsCount));
    oper.setProperty(EventGenerator.ROLLING_WINDOW_COUNT, String.valueOf(this.generatorWindowCount));
    return oper;
  }

  public OperatorInstance getAdViewsStampOperator(String name, DAG b) {
    OperatorInstance oper = b.addOperator(name, LoadClassifier.class);
    oper.setProperty(LoadClassifier.KEY_KEYS, "sprint,etrade,nike");
    return oper;
  }

  public OperatorInstance getInsertClicksOperator(String name, DAG b) {
    OperatorInstance oper = b.addOperator(name, FilterClassifier.class);
    oper.setProperty(FilterClassifier.KEY_KEYS, "sprint,etrade,nike");
    oper.setProperty(FilterClassifier.KEY_WEIGHTS, "home:60,10,30;finance:10,75,15;sports:10,10,80;mail:50,15,35"); // ctr in ratio
    // Getting $1,$5,$4 per click respectively
    oper.setProperty(FilteredEventClassifier.KEY_VALUES, "1,5,4");
    oper.setProperty(FilteredEventClassifier.KEY_FILTER, "40,1000"); // average value for each classify_key, i.e. money paid by advertizer
    return oper;
  }

  public OperatorInstance getStreamMerger10Operator(String name, DAG b) {
    OperatorInstance oper = b.addOperator(name, StreamMerger10.class);
    return oper;
  }

  public OperatorInstance getStreamMergerOperator(String name, DAG b) {
    OperatorInstance oper = b.addOperator(name, StreamMerger.class);
    return oper;
  }


  @Override
  public DAG getApplication(Configuration conf) {

    configure(conf);

    DAG dag = new DAG(conf);

    OperatorInstance viewAggrSum10 = getStreamMerger10Operator("viewaggregatesum", dag);
    OperatorInstance clickAggrSum10 = getStreamMerger10Operator("clickaggregatesum", dag);
    OperatorInstance viewAggrCount10 = getStreamMerger10Operator("viewaggregatecount", dag);
    OperatorInstance clickAggrCount10 = getStreamMerger10Operator("clickaggregatecount", dag);

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

      OperatorInstance viewGen = getPageViewGenOperator(viewgenstr, dag);
      OperatorInstance adviews = getAdViewsStampOperator(adviewstr, dag);
      OperatorInstance insertclicks = getInsertClicksOperator(insertclicksstr, dag);
      OperatorInstance viewAggregate = getSumOperator(viewAggrstr, dag);
      OperatorInstance clickAggregate = getSumOperator(clickAggrstr, dag);

      dag.addStream(viewsstreamstr, viewGen.getOutput(EventGenerator.OPORT_DATA), adviews.getInput(EventClassifier.IPORT_IN_DATA)).setInline(true);
      dag.addStream(viewsaggregatesrteamstr, adviews.getOutput(EventClassifier.OPORT_OUT_DATA), insertclicks.getInput(FilteredEventClassifier.IPORT_IN_DATA),
                    viewAggregate.getInput(Sum.IPORT_DATA)).setInline(true);
      dag.addStream(clicksaggregatestreamstr, insertclicks.getOutput(FilteredEventClassifier.OPORT_OUT_DATA), clickAggregate.getInput(Sum.IPORT_DATA)).setInline(true);

      dag.addStream(viewaggrsumstreamstr, viewAggregate.getOutput(Sum.OPORT_SUM), viewAggrSum10.getInput(StreamMerger10.getInputName(i)));
      dag.addStream(clickaggrsumstreamstr, clickAggregate.getOutput(Sum.OPORT_SUM), clickAggrSum10.getInput(StreamMerger10.getInputName(i)));
      dag.addStream(viewaggrcountstreamstr, viewAggregate.getOutput(Sum.OPORT_COUNT), viewAggrCount10.getInput(StreamMerger10.getInputName(i)));
      dag.addStream(clickaggrcountstreamstr, clickAggregate.getOutput(Sum.OPORT_COUNT), clickAggrCount10.getInput(StreamMerger10.getInputName(i)));
    }

    OperatorInstance ctr = getQuotientOperator("ctr", dag);
    OperatorInstance cost = getSumOperator("cost", dag);
    OperatorInstance revenue = getSumOperator("rev", dag);
    OperatorInstance margin = getMarginOperator("margin", dag);
    OperatorInstance merge = getStreamMergerOperator("countmerge", dag);
    OperatorInstance tuple_counter = getThroughputCounter("tuple_counter", dag);

    dag.addStream("adviewsdata", viewAggrSum10.getOutput(StreamMerger10.OPORT_OUT_DATA), cost.getInput(Sum.IPORT_DATA));
    dag.addStream("clicksdata", clickAggrSum10.getOutput(StreamMerger10.OPORT_OUT_DATA), revenue.getInput(Sum.IPORT_DATA));
    dag.addStream("viewtuplecount", viewAggrCount10.getOutput(StreamMerger10.OPORT_OUT_DATA), ctr.getInput(Quotient.IPORT_DENOMINATOR)
            , merge.getInput(StreamMerger.IPORT_IN_DATA1));
    dag.addStream("clicktuplecount", clickAggrCount10.getOutput(StreamMerger10.OPORT_OUT_DATA), ctr.getInput(Quotient.IPORT_NUMERATOR)
            , merge.getInput(StreamMerger.IPORT_IN_DATA2));
    dag.addStream("total count", merge.getOutput(StreamMerger.OPORT_OUT_DATA), tuple_counter.getInput(ThroughputCounter.IPORT_DATA));


    OperatorInstance revconsole = getConsoleOperator(dag, "revConsole");
    OperatorInstance costconsole = getConsoleOperator(dag, "costConsole");
    OperatorInstance marginconsole = getConsoleOperator(dag, "marginConsole");
    OperatorInstance ctrconsole = getConsoleOperator(dag, "ctrConsole");
    OperatorInstance viewcountconsole = getConsoleOperator(dag, "viewCountConsole");

    dag.addStream("revenuedata", revenue.getOutput(Sum.OPORT_SUM), margin.getInput(Margin.IPORT_DENOMINATOR), revconsole.getInput(ConsoleOutputOperator.INPUT)).setInline(allInline);
    dag.addStream("costdata", cost.getOutput(Sum.OPORT_SUM), margin.getInput(Margin.IPORT_NUMERATOR), costconsole.getInput(ConsoleOutputOperator.INPUT)).setInline(allInline);
    dag.addStream("margindata", margin.getOutput(Margin.OPORT_MARGIN), marginconsole.getInput(ConsoleOutputOperator.INPUT)).setInline(allInline);
    dag.addStream("ctrdata", ctr.getOutput(Quotient.OPORT_QUOTIENT), ctrconsole.getInput(ConsoleOutputOperator.INPUT)).setInline(allInline);
    dag.addStream("tuplecount", tuple_counter.getOutput(ThroughputCounter.OPORT_COUNT) , viewcountconsole.getInput(ConsoleOutputOperator.INPUT)).setInline(allInline);

    return dag;
  }
}

