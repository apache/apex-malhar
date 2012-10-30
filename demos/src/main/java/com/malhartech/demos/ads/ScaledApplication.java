/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.ads;

import com.malhartech.api.DAG;
import com.malhartech.api.Operator;
import com.malhartech.dag.ApplicationFactory;
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
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;

/**
 * Example of application configuration in Java using {@link com.malhartech.stram.conf.NewDAGBuilder}.<p>
 */
public class ScaledApplication implements ApplicationFactory
{
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

  public void setUnitTestMode()
  {
    generatorVTuplesBlast = 10;
    generatorWindowCount = 25;
    generatorMaxWindowsCount = 20;
  }

  public void setLocalMode()
  {
    generatorVTuplesBlast = 1000; // keep this number low to not distort window boundaries
    generatorWindowCount = 25;
    //generatorMaxWindowsCount = 50;
    generatorMaxWindowsCount = 1000;
  }

  private void configure(Configuration conf)
  {

    if (LAUNCHMODE_YARN.equals(conf.get(DAG.STRAM_LAUNCH_MODE))) {
      setLocalMode();
      // settings only affect distributed mode
      conf.setIfUnset(DAG.STRAM_CONTAINER_MEMORY_MB, "2048");
      conf.setIfUnset(DAG.STRAM_MASTER_MEMORY_MB, "1024");
      conf.setIfUnset(DAG.STRAM_MAX_CONTAINERS, "1");
    }
    else if (LAUNCHMODE_LOCAL.equals(conf.get(DAG.STRAM_LAUNCH_MODE))) {
      setLocalMode();
    }

    this.numGenerators = conf.getInt(P_numGenerators, this.numGenerators);
    this.generatorVTuplesBlast = conf.getInt(P_generatorVTuplesBlast, this.generatorVTuplesBlast);
    this.generatorMaxWindowsCount = conf.getInt(P_generatorMaxWindowsCount, this.generatorMaxWindowsCount);
    this.allInline = conf.getBoolean(P_allInline, this.allInline);

  }

  private Operator getConsoleOperator(String name, DAG b)
  {
    // output to HTTP server when specified in environment setting
    Operator ret;
    String serverAddr = System.getenv("MALHAR_AJAXSERVER_ADDRESS");
    if (serverAddr != null) {
      HttpOutputOperator oper = b.addOperator(name, HttpOutputOperator.class);
      URI u = null;
      try {
        u = new URI("http://" + serverAddr + "/channel/" + name);
      }
      catch (URISyntaxException ex) {
        Logger.getLogger(Application.class.getName()).log(Level.SEVERE, null, ex);
      }
      oper.setResourceURL(u);
      ret = oper;
    }
    else {
      ConsoleOutputOperator oper = b.addOperator(name, ConsoleOutputOperator.class);
      oper.setStringFormat(name + ": %s");
      ret = oper;
    }
    return ret;
  }

  public Operator getSumOperator(String name, DAG b)
  {
    return b.addOperator(name, Sum.class);
  }

  public Operator getThroughputCounter(String name, DAG b)
  {
    ThroughputCounter oper = b.addOperator(name, ThroughputCounter.class);
    oper.setRollingWindowCount(this.generatorWindowCount);
    return oper;
  }

  public Operator getMarginOperator(String name, DAG b)
  {
    Margin oper = b.addOperator(name, Margin.class);
    oper.setPercent(true);
    return oper;
  }

  public Operator getQuotientOperator(String name, DAG b)
  {
    Quotient oper = b.addOperator(name, Quotient.class);
    oper.setMult_by(100);
    oper.setDokey(true);
    return oper;
  }

  public Operator getPageViewGenOperator(String name, DAG b)
  {
    EventGenerator oper = b.addOperator(name, EventGenerator.class);
    oper.setKeys("home,finance,sports,mail");
    // Paying $2.15,$3,$1.75,$.6 for 1000 views respectively
    oper.setValues("0.00215,0.003,0.00175,0.0006");
    oper.setWeights("25,25,25,25");
    oper.setTuplesBlast(this.generatorVTuplesBlast);
    oper.setMaxcountofwindows(generatorMaxWindowsCount);
    oper.setRollingWindowCount(this.generatorWindowCount);
    return oper;
  }

  public Operator getAdViewsStampOperator(String name, DAG b)
  {
    EventClassifier oper = b.addOperator(name, EventClassifier.class);
    HashMap<String, Double> kmap = new HashMap<String, Double>();
    kmap.put("sprint", 0.0);
    kmap.put("etrace", 0.0);
    kmap.put("nike", 0.0);
    oper.setKeyMap(kmap);
    return oper;
  }

  public Operator getInsertClicksOperator(String name, DAG b)
  {
    FilteredEventClassifier<Double> oper = b.addOperator(name, new FilteredEventClassifier<Double>());
    HashMap<String, Double> kmap = new HashMap<String, Double>();
    // Getting $1,$5,$4 per click respectively
    kmap.put("sprint", 1.0);
    kmap.put("etrade", 5.0);
    kmap.put("nike", 4.0);

    HashMap<String, ArrayList<Integer>> wmap = new HashMap<String, ArrayList<Integer>>();
    ArrayList<Integer> alist = new ArrayList<Integer>(3);
    alist.add(60);
    alist.add(10);
    alist.add(30);
    wmap.put("home", alist);
    alist = new ArrayList<Integer>(3);
    alist.add(10);
    alist.add(75);
    alist.add(15);
    wmap.put("finance", alist);
    alist = new ArrayList<Integer>(3);
    alist.add(10);
    alist.add(10);
    alist.add(80);
    wmap.put("sports", alist);
    alist.add(50);
    alist.add(15);
    alist.add(35);
    wmap.put("mail", alist);
    oper.setKeyWeights(wmap);
    oper.setPassFilter(40);
    oper.setTotalFilter(1000);
    return oper;
  }

  public Operator getStreamMerger10Operator(String name, DAG b)
  {
    return b.addOperator(name, StreamMerger10.class);
  }

  public Operator getStreamMergerOperator(String name, DAG b)
  {
    return b.addOperator(name, StreamMerger.class);
  }

  @Override
  public DAG getApplication(Configuration conf)
  {

    configure(conf);

    DAG dag = new DAG(conf);

    Operator viewAggrSum10 = getStreamMerger10Operator("viewaggregatesum", dag);
    Operator clickAggrSum10 = getStreamMerger10Operator("clickaggregatesum", dag);
    Operator viewAggrCount10 = getStreamMerger10Operator("viewaggregatecount", dag);
    Operator clickAggrCount10 = getStreamMerger10Operator("clickaggregatecount", dag);

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

      dag.addStream(viewsstreamstr, viewGen.getOutput(EventGenerator.OPORT_DATA), adviews.getInput(EventClassifier.IPORT_IN_DATA)).setInline(true);
      dag.addStream(viewsaggregatesrteamstr, adviews.getOutput(EventClassifier.OPORT_OUT_DATA), insertclicks.getInput(FilteredEventClassifier.IPORT_IN_DATA),
                    viewAggregate.getInput(Sum.IPORT_DATA)).setInline(true);
      dag.addStream(clicksaggregatestreamstr, insertclicks.getOutput(FilteredEventClassifier.OPORT_OUT_DATA), clickAggregate.getInput(Sum.IPORT_DATA)).setInline(true);

      dag.addStream(viewaggrsumstreamstr, viewAggregate.getOutput(Sum.OPORT_SUM), viewAggrSum10.getInput(StreamMerger10.getInputName(i)));
      dag.addStream(clickaggrsumstreamstr, clickAggregate.getOutput(Sum.OPORT_SUM), clickAggrSum10.getInput(StreamMerger10.getInputName(i)));
      dag.addStream(viewaggrcountstreamstr, viewAggregate.getOutput(Sum.OPORT_COUNT), viewAggrCount10.getInput(StreamMerger10.getInputName(i)));
      dag.addStream(clickaggrcountstreamstr, clickAggregate.getOutput(Sum.OPORT_COUNT), clickAggrCount10.getInput(StreamMerger10.getInputName(i)));
    }

    Operator ctr = getQuotientOperator("ctr", dag);
    Operator cost = getSumOperator("cost", dag);
    Operator revenue = getSumOperator("rev", dag);
    Operator margin = getMarginOperator("margin", dag);
    Operator merge = getStreamMergerOperator("countmerge", dag);
    Operator tuple_counter = getThroughputCounter("tuple_counter", dag);

    dag.addStream("adviewsdata", viewAggrSum10.getOutput(StreamMerger10.OPORT_OUT_DATA), cost.getInput(Sum.IPORT_DATA));
    dag.addStream("clicksdata", clickAggrSum10.getOutput(StreamMerger10.OPORT_OUT_DATA), revenue.getInput(Sum.IPORT_DATA));
    dag.addStream("viewtuplecount", viewAggrCount10.getOutput(StreamMerger10.OPORT_OUT_DATA), ctr.getInput(Quotient.IPORT_DENOMINATOR), merge.getInput(StreamMerger.IPORT_IN_DATA1));
    dag.addStream("clicktuplecount", clickAggrCount10.getOutput(StreamMerger10.OPORT_OUT_DATA), ctr.getInput(Quotient.IPORT_NUMERATOR), merge.getInput(StreamMerger.IPORT_IN_DATA2));
    dag.addStream("total count", merge.getOutput(StreamMerger.OPORT_OUT_DATA), tuple_counter.getInput(ThroughputCounter.IPORT_DATA));


    Operator revconsole = getConsoleOperator("revConsole", dag);
    Operator costconsole = getConsoleOperator("costConsole", dag);
    Operator marginconsole = getConsoleOperator("marginConsole", dag);
    Operator ctrconsole = getConsoleOperator("ctrConsole", dag);
    Operator viewcountconsole = getConsoleOperator("viewCountConsole", dag);

    dag.addStream("revenuedata", revenue.getOutput(Sum.OPORT_SUM), margin.getInput(Margin.IPORT_DENOMINATOR), revconsole.getInput(ConsoleOutputOperator.INPUT)).setInline(allInline);
    dag.addStream("costdata", cost.getOutput(Sum.OPORT_SUM), margin.getInput(Margin.IPORT_NUMERATOR), costconsole.getInput(ConsoleOutputOperator.INPUT)).setInline(allInline);
    dag.addStream("margindata", margin.getOutput(Margin.OPORT_MARGIN), marginconsole.getInput(ConsoleOutputOperator.INPUT)).setInline(allInline);
    dag.addStream("ctrdata", ctr.getOutput(Quotient.OPORT_QUOTIENT), ctrconsole.getInput(ConsoleOutputOperator.INPUT)).setInline(allInline);
    dag.addStream("tuplecount", tuple_counter.getOutput(ThroughputCounter.OPORT_COUNT), viewcountconsole.getInput(ConsoleOutputOperator.INPUT)).setInline(allInline);

    return dag;
  }
}
