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
import com.malhartech.lib.stream.StreamMerger5;
import com.malhartech.lib.testbench.EventClassifier;
import com.malhartech.lib.testbench.EventGenerator;
import com.malhartech.lib.testbench.FilteredEventClassifier;
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

  private HttpOutputOperator<HashMap<String, Double>> getHttpOutputDoubleOperator(DAG b, String name)
  {
    // output to HTTP server when specified in environment setting
    String serverAddr = System.getenv("MALHAR_AJAXSERVER_ADDRESS");
    HttpOutputOperator<HashMap<String, Double>> oper = b.addOperator(name, new HttpOutputOperator<HashMap<String, Double>>());
    URI u = null;
    try {
      u = new URI("http://" + serverAddr + "/channel/" + name);
    }
    catch (URISyntaxException ex) {
      Logger.getLogger(Application.class.getName()).log(Level.SEVERE, null, ex);
    }
    oper.setResourceURL(u);
    return oper;
  }

  private HttpOutputOperator<HashMap<String, Number>> getHttpOutputNumberOperator(DAG b, String name)
  {
    // output to HTTP server when specified in environment setting
    String serverAddr = System.getenv("MALHAR_AJAXSERVER_ADDRESS");
    HttpOutputOperator<HashMap<String, Number>> oper = b.addOperator(name, new HttpOutputOperator<HashMap<String, Number>>());
    URI u = null;
    try {
      u = new URI("http://" + serverAddr + "/channel/" + name);
    }
    catch (URISyntaxException ex) {
      Logger.getLogger(Application.class.getName()).log(Level.SEVERE, null, ex);
    }
    oper.setResourceURL(u);
    return oper;
  }

  private ConsoleOutputOperator<HashMap<String, Number>> getConsoleNumberOperator(DAG b, String name)
  {
    // output to HTTP server when specified in environment setting
    ConsoleOutputOperator<HashMap<String, Number>> oper = b.addOperator(name, new ConsoleOutputOperator<HashMap<String, Number>>());
    oper.setStringFormat(name + ": %s");
    return oper;
  }

  private ConsoleOutputOperator<HashMap<String, Double>> getConsoleDoubleOperator(DAG b, String name)
  {
    // output to HTTP server when specified in environment setting
    ConsoleOutputOperator<HashMap<String, Double>> oper = b.addOperator(name, new ConsoleOutputOperator<HashMap<String, Double>>());
    oper.setStringFormat(name + ": %s");
    return oper;
  }

  public Sum<String, Double> getSumOperator(String name, DAG b)
  {
    return b.addOperator(name, new Sum<String, Double>());
  }

  public ThroughputCounter<String> getThroughputCounter(String name, DAG b)
  {
    ThroughputCounter<String> oper = b.addOperator(name, new ThroughputCounter<String>());
    oper.setRollingWindowCount(5);
    return oper;
  }

  public Margin<String, Double> getMarginOperator(String name, DAG b)
  {
    Margin<String, Double> oper = b.addOperator(name, new Margin<String, Double>());
    oper.setPercent(true);
    return oper;
  }

  public Quotient<String, Integer> getQuotientOperator(String name, DAG b)
  {
    Quotient<String, Integer> oper = b.addOperator(name, new Quotient<String, Integer>());
    oper.setMult_by(100);
    oper.setCountkey(true);
    return oper;
  }

  public EventGenerator getPageViewGenOperator(String name, DAG b)
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

  public EventClassifier getAdViewsStampOperator(String name, DAG b)
  {
    EventClassifier oper = b.addOperator(name, EventClassifier.class);
    HashMap<String, Double> kmap = new HashMap<String, Double>();
    kmap.put("sprint", null);
    kmap.put("etrade", null);
    kmap.put("nike", null);
    oper.setKeyMap(kmap);
    return oper;
  }

  public FilteredEventClassifier<Double> getInsertClicksOperator(String name, DAG b)
  {
    FilteredEventClassifier<Double> oper = b.addOperator(name, new FilteredEventClassifier<Double>());
    HashMap<String, Double> kmap = new HashMap<String, Double>();
    // Getting $1,$5,$4 per click respectively
    kmap.put("sprint", 1.0);
    kmap.put("etrade", 5.0);
    kmap.put("nike", 4.0);
   oper. setKeyMap(kmap);

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
    alist = new ArrayList<Integer>(3);
    alist.add(50);
    alist.add(15);
    alist.add(35);
    wmap.put("mail", alist);
    oper.setKeyWeights(wmap);
    oper.setPassFilter(40);
    oper.setTotalFilter(1000);
    return oper;
  }

  public StreamMerger10<HashMap<String, Double>> getStreamMerger10DoubleOperator(String name, DAG b)
  {
    return b.addOperator(name, new StreamMerger10<HashMap<String, Double>>());
  }

  public StreamMerger10<HashMap<String, Integer>> getStreamMerger10IntegerOperator(String name, DAG b)
  {
    return b.addOperator(name, new StreamMerger10<HashMap<String, Integer>>());
  }

  public StreamMerger<HashMap<String, Integer>> getStreamMerger(String name, DAG b)
  {
    return b.addOperator(name, new StreamMerger<HashMap<String, Integer>>());
  }

  @Override
  public DAG getApplication(Configuration conf)
  {

    configure(conf);
    DAG dag = new DAG(conf);

    StreamMerger5<HashMap<String, Double>> viewAggrSum10 = getStreamMerger10DoubleOperator("viewaggregatesum", dag);
    StreamMerger5<HashMap<String, Double>> clickAggrSum10 = getStreamMerger10DoubleOperator("clickaggregatesum", dag);
    StreamMerger5<HashMap<String, Integer>> viewAggrCount10 = getStreamMerger10IntegerOperator("viewaggregatecount", dag);
    StreamMerger5<HashMap<String, Integer>> clickAggrCount10 = getStreamMerger10IntegerOperator("clickaggregatecount", dag);

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

      EventGenerator viewGen = getPageViewGenOperator(viewgenstr, dag);
      EventClassifier adviews = getAdViewsStampOperator(adviewstr, dag);
      FilteredEventClassifier<Double> insertclicks = getInsertClicksOperator(insertclicksstr, dag);
      Sum<String, Double> viewAggregate = getSumOperator(viewAggrstr, dag);
      Sum<String, Double> clickAggregate = getSumOperator(clickAggrstr, dag);

      dag.addStream(viewsstreamstr, viewGen.hash_data, adviews.event).setInline(true);
      dag.addStream(viewsaggregatesrteamstr, adviews.data, insertclicks.data, viewAggregate.data).setInline(true);
      dag.addStream(clicksaggregatestreamstr, insertclicks.filter, clickAggregate.data).setInline(true);

      dag.addStream(viewaggrsumstreamstr, viewAggregate.sum, viewAggrSum10.getInputPort(i));
      dag.addStream(clickaggrsumstreamstr, clickAggregate.sum, clickAggrSum10.getInputPort(i));
      dag.addStream(viewaggrcountstreamstr, viewAggregate.count, viewAggrCount10.getInputPort(i));
      dag.addStream(clickaggrcountstreamstr, clickAggregate.count, clickAggrCount10.getInputPort(i));
    }

    Quotient<String, Integer> ctr = getQuotientOperator("ctr", dag);
    Sum<String, Double> cost = getSumOperator("cost", dag);
    Sum<String, Double> revenue = getSumOperator("rev", dag);
    Margin<String, Double> margin = getMarginOperator("margin", dag);
    StreamMerger<HashMap<String, Integer>> merge = getStreamMerger("countmerge", dag);
    ThroughputCounter<String> tuple_counter = getThroughputCounter("tuple_counter", dag);

    dag.addStream("adviewsdata", viewAggrSum10.out, cost.data);
    dag.addStream("clicksdata", clickAggrSum10.out, revenue.data);
    dag.addStream("viewtuplecount", viewAggrCount10.out, ctr.denominator, merge.data1);
    dag.addStream("clicktuplecount", clickAggrCount10.out, ctr.numerator, merge.data2);
    dag.addStream("total count", merge.out, tuple_counter.data);

    String serverAddr = System.getenv("MALHAR_AJAXSERVER_ADDRESS");
    if (serverAddr == null) {
      ConsoleOutputOperator<HashMap<String, Double>> revconsole = getConsoleDoubleOperator(dag, "revConsole");
      ConsoleOutputOperator<HashMap<String, Double>> costconsole = getConsoleDoubleOperator(dag, "costConsole");
      ConsoleOutputOperator<HashMap<String, Double>> marginconsole = getConsoleDoubleOperator(dag, "marginConsole");
      ConsoleOutputOperator<HashMap<String, Double>> ctrconsole = getConsoleDoubleOperator(dag, "ctrConsole");
      ConsoleOutputOperator<HashMap<String, Number>> viewcountconsole = getConsoleNumberOperator(dag, "viewCountConsole");
      dag.addStream("revenuedata", revenue.sum, margin.denominator, revconsole.input).setInline(allInline);
      dag.addStream("costdata", cost.sum, margin.numerator, costconsole.input).setInline(allInline);
      dag.addStream("margindata", margin.margin, marginconsole.input).setInline(allInline);
      dag.addStream("ctrdata", ctr.quotient, ctrconsole.input).setInline(allInline);
      dag.addStream("tuplecount", tuple_counter.count, viewcountconsole.input).setInline(allInline);
    }
    else {
      HttpOutputOperator<HashMap<String, Double>> revhttp = getHttpOutputDoubleOperator(dag, "revConsole");
      HttpOutputOperator<HashMap<String, Double>> costhttp = getHttpOutputDoubleOperator(dag, "costConsole");
      HttpOutputOperator<HashMap<String, Double>> marginhttp = getHttpOutputDoubleOperator(dag, "marginConsole");
      HttpOutputOperator<HashMap<String, Double>> ctrhttp = getHttpOutputDoubleOperator(dag, "ctrConsole");
      HttpOutputOperator<HashMap<String, Number>> viewcounthttp = getHttpOutputNumberOperator(dag, "viewCountConsole");
      dag.addStream("revenuedata", revenue.sum, margin.denominator, revhttp.input).setInline(allInline);
      dag.addStream("costdata", cost.sum, margin.numerator, costhttp.input).setInline(allInline);
      dag.addStream("margindata", margin.margin, marginhttp.input).setInline(allInline);
      dag.addStream("ctrdata", ctr.quotient, ctrhttp.input).setInline(allInline);
      dag.addStream("tuplecount", tuple_counter.count, viewcounthttp.input).setInline(allInline);
    }
    return dag;
  }
}
