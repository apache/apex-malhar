/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.ads;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.malhartech.dag.ApplicationFactory;
import com.malhartech.dag.DAG;
import com.malhartech.dag.DAG.Operator;
import com.malhartech.dag.DAG.StreamDecl;
import com.malhartech.lib.io.ConsoleOutputModule;
import com.malhartech.lib.io.HdfsOutputModule;
import com.malhartech.lib.io.HttpOutputModule;
import com.malhartech.lib.math.ArithmeticMargin;
import com.malhartech.lib.math.ArithmeticQuotient;
import com.malhartech.lib.math.ArithmeticSum;
import com.malhartech.lib.testbench.FilterClassifier;
import com.malhartech.lib.testbench.LoadClassifier;
import com.malhartech.lib.testbench.LoadGenerator;
import com.malhartech.lib.testbench.StreamMerger;
import com.malhartech.lib.testbench.ThroughputCounter;


/**
 * Example of application configuration in Java using {@link com.malhartech.stram.conf.NewDAGBuilder}.<p>
 */

public class Application implements ApplicationFactory {

  public static final String P_generatorVTuplesBlast = Application.class.getName() + ".generatorVTuplesBlast";
  public static final String P_generatorMaxWindowsCount = Application.class.getName() + ".generatorMaxWindowsCount";
  public static final String P_allInline = Application.class.getName() + ".allInline";
  public static final String P_enableHdfs = Application.class.getName() + ".enableHdfs";

  // adjust these depending on execution mode (junit, cli-local, cluster)
  private int generatorVTuplesBlast = 1000;
  private int generatorMaxWindowsCount = 100;
  private int generatorWindowCount = 1;
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

    this.generatorVTuplesBlast = conf.getInt(P_generatorVTuplesBlast, this.generatorVTuplesBlast);
    this.generatorMaxWindowsCount = conf.getInt(P_generatorMaxWindowsCount, this.generatorMaxWindowsCount);
    this.allInline = conf.getBoolean(P_allInline, this.allInline);

  }

  /**
   * Map properties from application to operator scope
   * @param dag
   * @param op
   */
  public static Map<String, String> getOperatorProperties(Configuration appConf, Class<?> appClass, String operatorId) {
    String keyPrefix = appClass.getName() + "." + operatorId + ".";
    Map<String, String> values = appConf.getValByRegex(keyPrefix + "*");
    Map<String, String> properties = new HashMap<String, String>(values.size());
    for (Map.Entry<String, String> e : values.entrySet()) {
      properties.put(e.getKey().replace(keyPrefix, ""), e.getValue());
    }
    return properties;
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

  private DAG.InputPort getViewsToHdfsOperator(DAG dag, String operatorName)
  {
    Map<String, String> props = getOperatorProperties(dag.getConf(), this.getClass(), operatorName);
    Operator o = dag.addOperator(operatorName, HdfsOutputModule.class);
    o.setProperty(HdfsOutputModule.KEY_APPEND, "false");
    o.setProperty(HdfsOutputModule.KEY_FILEPATH, "file:///tmp/adsdemoviews-%(moduleId)-part%(partIndex)");
    for (Map.Entry<String, String> e : props.entrySet()) {
      o.setProperty(e.getKey(), e.getValue());
    }
    return o.getInput(HdfsOutputModule.INPUT);
  }

  public Operator getSumOperator(String name, DAG b, String debug) {
    Operator oper = b.addOperator(name, ArithmeticSum.class);
    oper.setProperty("do_debug", debug);

    return oper;
  }

  public Operator getStreamMerger(String name, DAG b) {
    return b.addOperator(name, StreamMerger.class);
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

  @Override
  public DAG getApplication(Configuration conf) {

    configure(conf);

    DAG dag = new DAG(conf);

    Operator viewGen = getPageViewGenOperator("viewGen", dag);
    Operator adviews = getAdViewsStampOperator("adviews", dag);
    Operator insertclicks = getInsertClicksOperator("insertclicks", dag);
    Operator viewAggregate = getSumOperator("viewAggr", dag, "");
    Operator clickAggregate = getSumOperator("clickAggr", dag, "");

    Operator ctr = getQuotientOperator("ctr", dag);
    Operator cost = getSumOperator("cost", dag, "");
    Operator revenue = getSumOperator("rev", dag, "");
    Operator margin = getMarginOperator("margin", dag);

    Operator merge = getStreamMerger("countmerge", dag);
    Operator tuple_counter = getThroughputCounter("tuple_counter", dag);

    dag.addStream("views", viewGen.getOutput(LoadGenerator.OPORT_DATA), adviews.getInput(LoadClassifier.IPORT_IN_DATA)).setInline(true);
    StreamDecl viewsAggStream = dag.addStream("viewsaggregate", adviews.getOutput(LoadClassifier.OPORT_OUT_DATA), insertclicks.getInput(FilterClassifier.IPORT_IN_DATA),
                      viewAggregate.getInput(ArithmeticSum.IPORT_DATA)).setInline(true);

    if (conf.getBoolean(P_enableHdfs, false)) {
      DAG.InputPort viewsToHdfs = getViewsToHdfsOperator(dag, "viewsToHdfs");
      viewsAggStream.addSink(viewsToHdfs);
    }

    dag.addStream("clicksaggregate", insertclicks.getOutput(FilterClassifier.OPORT_OUT_DATA), clickAggregate.getInput(ArithmeticSum.IPORT_DATA)).setInline(true);

    dag.addStream("adviewsdata", viewAggregate.getOutput(ArithmeticSum.OPORT_SUM), cost.getInput(ArithmeticSum.IPORT_DATA));
    dag.addStream("clicksdata", clickAggregate.getOutput(ArithmeticSum.OPORT_SUM), revenue.getInput(ArithmeticSum.IPORT_DATA));
    dag.addStream("viewtuplecount", viewAggregate.getOutput(ArithmeticSum.OPORT_COUNT), ctr.getInput(ArithmeticQuotient.IPORT_DENOMINATOR)
            , merge.getInput(StreamMerger.IPORT_IN_DATA1));
    dag.addStream("clicktuplecount", clickAggregate.getOutput(ArithmeticSum.OPORT_COUNT), ctr.getInput(ArithmeticQuotient.IPORT_NUMERATOR)
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

