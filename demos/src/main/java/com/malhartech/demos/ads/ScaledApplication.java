/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.ads;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;

import com.malhartech.api.DAG;
import com.malhartech.api.Operator.InputPort;
import com.malhartech.api.ApplicationFactory;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.io.HdfsOutputOperator;
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

/**
 * Example of application configuration in Java using {@link com.malhartech.stram.conf.NewDAGBuilder}.<p>
 */
public class ScaledApplication implements ApplicationFactory
{
  public static final String P_numGenerators = ScaledApplication.class.getName() + ".numGenerators";
  public static final String P_generatorVTuplesBlast = ScaledApplication.class.getName() + ".generatorVTuplesBlast";
  public static final String P_generatorMaxWindowsCount = ScaledApplication.class.getName() + ".generatorMaxWindowsCount";
  public static final String P_allInline = ScaledApplication.class.getName() + ".allInline";
  public static final String P_enableHdfs = ScaledApplication.class.getName() + ".enableHdfs";

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

  private <T extends Number> InputPort<HashMap<String, T>> getConsolePort(DAG b, String name, boolean silent)
  {
    // output to HTTP server when specified in environment setting
    String serverAddr = System.getenv("MALHAR_AJAXSERVER_ADDRESS");
    if (serverAddr == null) {
      ConsoleOutputOperator<HashMap<String, T>> oper = b.addOperator(name, new ConsoleOutputOperator<HashMap<String, T>>());
      oper.setStringFormat(name + "(%d): %s");
      oper.silent = silent;
      return oper.input;
    }
    HttpOutputOperator<HashMap<String, T>> oper = b.addOperator(name, new HttpOutputOperator<HashMap<String, T>>());
    URI u = URI.create("http://" + serverAddr + "/channel/" + name);
    oper.setResourceURL(u);
    return oper.input;
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
    oper.setMinCount(5000); // Only report if at least 5000 tuples are processed in a window to avoid noise
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
      EventGenerator viewGen = getPageViewGenOperator("viewGen"+i, dag);
      EventClassifier adviews = getAdViewsStampOperator("adviews"+i, dag);
      FilteredEventClassifier<Double> insertclicks = getInsertClicksOperator("insertclicks"+i, dag);
      Sum<String, Double> viewAggregate = getSumOperator("viewAggr"+i, dag);
      Sum<String, Double> clickAggregate = getSumOperator("clickAggr"+i, dag);

      dag.addStream("views"+i, viewGen.hash_data, adviews.event).setInline(true);
      DAG.StreamDecl viewsAggStream = dag.addStream("viewsaggregate"+i, adviews.data, insertclicks.data, viewAggregate.data).setInline(true);

      if (conf.getBoolean(P_enableHdfs, false)) {
        HdfsOutputOperator<HashMap<String, Double>> viewsToHdfs = dag.addOperator("viewsToHdfs"+i, new HdfsOutputOperator<HashMap<String, Double>>());
        viewsToHdfs.setAppend(false);
        viewsToHdfs.setFilePath("file:///tmp/adsdemo/views-%(operatorId)-part%(partIndex)");
        viewsAggStream.addSink(viewsToHdfs.input);
      }

      dag.addStream("clicksaggregate"+i, insertclicks.filter, clickAggregate.data).setInline(true);
      dag.addStream("viewsaggrsum"+i, viewAggregate.sum, viewAggrSum10.getInputPort(i));
      dag.addStream("clicksaggrsum"+i, clickAggregate.sum, clickAggrSum10.getInputPort(i));
      dag.addStream("viewsaggrcount"+i, viewAggregate.count, viewAggrCount10.getInputPort(i));
      dag.addStream("clicksaggrcount"+i, clickAggregate.count, clickAggrCount10.getInputPort(i));
    }

    Quotient<String, Integer> ctr = getQuotientOperator("ctr", dag);
    Sum<String, Double> cost = getSumOperator("cost", dag);
    Sum<String, Double> revenue = getSumOperator("rev", dag);
    Margin<String, Double> margin = getMarginOperator("margin", dag);
    StreamMerger<HashMap<String, Integer>> merge = getStreamMerger("countmerge", dag);
    ThroughputCounter<String> tuple_counter = getThroughputCounter("tuple_counter", dag);

    dag.addStream("adviewsdata", viewAggrSum10.out, cost.data);
    dag.addStream("clicksdata", clickAggrSum10.out, revenue.data);
    dag.addStream("viewtuplecount", viewAggrCount10.out, ctr.denominator, merge.data1).setInline(true);
    dag.addStream("clicktuplecount", clickAggrCount10.out, ctr.numerator, merge.data2).setInline(true);
    dag.addStream("total count", merge.out, tuple_counter.data).setInline(true);

    InputPort<HashMap<String, Double>> revconsole = getConsolePort(dag, "revConsole", false);
    InputPort<HashMap<String, Double>> costconsole = getConsolePort(dag, "costConsole", false);
    InputPort<HashMap<String, Double>> marginconsole = getConsolePort(dag, "marginConsole", false);
    InputPort<HashMap<String, Double>> ctrconsole = getConsolePort(dag, "ctrConsole", false);
    InputPort<HashMap<String, Number>> viewcountconsole = getConsolePort(dag, "viewCountConsole", false);

    dag.addStream("revenuedata", revenue.sum, margin.denominator, revconsole).setInline(allInline);
    dag.addStream("costdata", cost.sum, margin.numerator, costconsole).setInline(allInline);
    dag.addStream("margindata", margin.margin, marginconsole).setInline(allInline);
    dag.addStream("ctrdata", ctr.quotient, ctrconsole).setInline(allInline);
    dag.addStream("tuplecount", tuple_counter.count, viewcountconsole).setInline(allInline);

    return dag;
  }
}
