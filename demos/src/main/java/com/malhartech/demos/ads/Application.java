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
import com.malhartech.lib.testbench.EventClassifier;
import com.malhartech.lib.testbench.EventGenerator;
import com.malhartech.lib.testbench.FilteredEventClassifier;
import com.malhartech.lib.testbench.ThroughputCounter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Example of application configuration in Java using {@link com.malhartech.stram.conf.NewDAGBuilder}.<p>
 */
public class Application implements ApplicationFactory
{
  public static final String P_generatorVTuplesBlast = Application.class.getName() + ".generatorVTuplesBlast";
  public static final String P_generatorMaxWindowsCount = Application.class.getName() + ".generatorMaxWindowsCount";
  public static final String P_allInline = Application.class.getName() + ".allInline";
  public static final String P_enableHdfs = Application.class.getName() + ".enableHdfs";
  // adjust these depending on execution mode (junit, cli-local, cluster)
  private int generatorVTuplesBlast = 1000;
  private int generatorMaxWindowsCount = 100;
  private int generatorWindowCount = 1;
  private boolean allInline = true;

  public void setUnitTestMode()
  {
    generatorVTuplesBlast = 10;
    generatorWindowCount = 5;
    generatorMaxWindowsCount = 20;
  }

  public void setLocalMode()
  {
    generatorVTuplesBlast = 1000; // keep this number low to not distort window boundaries
    //generatorVTuplesBlast = 500000;
    generatorWindowCount = 5;
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

    this.generatorVTuplesBlast = conf.getInt(P_generatorVTuplesBlast, this.generatorVTuplesBlast);
    this.generatorMaxWindowsCount = conf.getInt(P_generatorMaxWindowsCount, this.generatorMaxWindowsCount);
    this.allInline = conf.getBoolean(P_allInline, this.allInline);

  }

  /**
   * Map properties from application to operator scope
   *
   * @param dag
   * @param op
   */
  public static Map<String, String> getOperatorInstanceProperties(Configuration appConf, Class<?> appClass, String operatorId)
  {
    String keyPrefix = appClass.getName() + "." + operatorId + ".";
    Map<String, String> values = appConf.getValByRegex(keyPrefix + "*");
    Map<String, String> properties = new HashMap<String, String>(values.size());
    for (Map.Entry<String, String> e: values.entrySet()) {
      properties.put(e.getKey().replace(keyPrefix, ""), e.getValue());
    }
    return properties;
  }

  private HttpOutputOperator<HashMap<String,Double>> getHttpOutputDoubleOperator(DAG b, String name)
  {
    // output to HTTP server when specified in environment setting
    String serverAddr = System.getenv("MALHAR_AJAXSERVER_ADDRESS");
    HttpOutputOperator<HashMap<String,Double>> oper = b.addOperator(name, new HttpOutputOperator<HashMap<String,Double>>());
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

    private HttpOutputOperator<HashMap<String,Number>> getHttpOutputNumberOperator(DAG b, String name)
  {
    // output to HTTP server when specified in environment setting
    String serverAddr = System.getenv("MALHAR_AJAXSERVER_ADDRESS");
    HttpOutputOperator<HashMap<String,Number>> oper = b.addOperator(name, new HttpOutputOperator<HashMap<String,Number>>());
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

  private ConsoleOutputOperator<HashMap<String,Number>> getConsoleNumberOperator(DAG b, String name)
  {
    // output to HTTP server when specified in environment setting
    ConsoleOutputOperator<HashMap<String,Number>> oper = b.addOperator(name, new ConsoleOutputOperator<HashMap<String,Number>>());
    oper.setStringFormat(name + ": %s");
    return oper;
  }

  private ConsoleOutputOperator<HashMap<String,Double>> getConsoleDoubleOperator(DAG b, String name)
  {
    // output to HTTP server when specified in environment setting
    ConsoleOutputOperator<HashMap<String,Double>> oper = b.addOperator(name, new ConsoleOutputOperator<HashMap<String,Double>>());
    oper.setStringFormat(name + ": %s");
    return oper;
  }
/*
  private DAG.InputPort getViewsToHdfsOperatorInstance(DAG dag, String operatorName)
  {
    Map<String, String> props = getOperatorInstanceProperties(dag.getConf(), this.getClass(), operatorName);
    OperatorInstance o = dag.addOperator(operatorName, HdfsOutputModule.class);
    o.setProperty(HdfsOutputModule.KEY_APPEND, "false");
    o.setProperty(HdfsOutputModule.KEY_FILEPATH, "file:///tmp/adsdemo/views-%(moduleId)-part%(partIndex)");
    for (Map.Entry<String, String> e: props.entrySet()) {
      o.setProperty(e.getKey(), e.getValue());
    }
    return o.getInput(HdfsOutputModule.INPUT);
  }
*/

  public Sum<String,Double> getSumOperator(String name, DAG b)
  {
    return b.addOperator(name, new Sum<String,Double>());
  }

  public StreamMerger<HashMap<String,Integer>> getStreamMerger(String name, DAG b)
  {
    StreamMerger<HashMap<String,Integer>> oper = b.addOperator(name, new StreamMerger<HashMap<String,Integer>>());
    return oper;
  }

  public ThroughputCounter<String> getThroughputCounter(String name, DAG b)
  {
    ThroughputCounter<String> oper = b.addOperator(name, new ThroughputCounter<String>());
    oper.setRollingWindowCount(5);
    return oper;
  }

  public Margin<String,Double> getMarginOperator(String name, DAG b)
  {
    Margin<String,Double> oper = b.addOperator(name, new Margin<String,Double>());
    oper.setPercent(true);
    return oper;
  }

  public Quotient<String,Integer> getQuotientOperator(String name, DAG b)
  {
    Quotient<String,Integer> oper = b.addOperator(name, new Quotient<String,Integer>());
    oper.setMult_by(100);
    oper.setDokey(true);
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
    kmap.put("sprint", 0.0);
    kmap.put("etrace", 0.0);
    kmap.put("nike", 0.0);
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

  @Override
  public DAG getApplication(Configuration conf)
  {

    configure(conf);
    DAG dag = new DAG();

    EventGenerator viewGen = getPageViewGenOperator("viewGen", dag);
    EventClassifier adviews = getAdViewsStampOperator("adviews", dag);
    FilteredEventClassifier<Double> insertclicks = getInsertClicksOperator("insertclicks", dag);
    Sum<String,Double> viewAggregate = getSumOperator("viewAggr", dag);
    Sum<String,Double> clickAggregate = getSumOperator("clickAggr", dag);

    Quotient<String,Integer> ctr = getQuotientOperator("ctr", dag);
    Sum<String,Double> cost = getSumOperator("cost", dag);
    Sum<String,Double> revenue = getSumOperator("rev", dag);
    Margin<String,Double> margin = getMarginOperator("margin", dag);

    StreamMerger<HashMap<String,Integer>> merge = getStreamMerger("countmerge", dag);
    ThroughputCounter<String> tuple_counter = getThroughputCounter("tuple_counter", dag);

    dag.addStream("views", viewGen.hash_data, adviews.event).setInline(true);
    dag.addStream("viewsaggregate", adviews.data, insertclicks.data, viewAggregate.data).setInline(true);

    /*
    if (conf.getBoolean(P_enableHdfs, false)) {
      DAG.InputPort viewsToHdfs = getViewsToHdfsOperatorInstance(dag, "viewsToHdfs");
      viewsAggStream.addSink(viewsToHdfs);
    }
*/
    dag.addStream("clicksaggregate", insertclicks.filter, clickAggregate.data).setInline(true);

    dag.addStream("adviewsdata", viewAggregate.sum, cost.data);
    dag.addStream("clicksdata", clickAggregate.sum, revenue.data);
    dag.addStream("viewtuplecount", viewAggregate.count, ctr.denominator, merge.data1);
    dag.addStream("clicktuplecount", clickAggregate.count, ctr.numerator, merge.data2);
    dag.addStream("total count", merge.out, tuple_counter.data);

    String serverAddr = System.getenv("MALHAR_AJAXSERVER_ADDRESS");
    if (serverAddr != null) {
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
