/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.mobile;

import com.malhartech.dag.ApplicationFactory;
import com.malhartech.dag.DAG;
import com.malhartech.dag.DAG.Operator;
import com.malhartech.lib.algo.InvertIndexMap;
import com.malhartech.lib.algo.TupleQueue;
import com.malhartech.lib.io.ConsoleOutputModule;
import com.malhartech.lib.io.HttpOutputModule;
import com.malhartech.lib.testbench.LoadIncrementer;
import com.malhartech.lib.testbench.LoadRandomGenerator;
import com.malhartech.lib.testbench.LoadSeedGenerator;
import com.malhartech.lib.testbench.SeedClassifier;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Example of application configuration in Java using {@link com.malhartech.stram.conf.NewDAGBuilder}.<p>
 */

public class Application implements ApplicationFactory {

  private static final Logger LOG = LoggerFactory.getLogger(Application.class);

  public static final String P_generatorVTuplesBlast = com.malhartech.demos.mobile.Application.class.getName() + ".generatorVTuplesBlast";
  public static final String P_generatorMaxWindowsCount = com.malhartech.demos.mobile.Application.class.getName() + ".generatorMaxWindowsCount";
  public static final String P_allInline = com.malhartech.demos.mobile.Application.class.getName() + ".allInline";

  // adjust these depending on execution mode (junit, cli-local, cluster)
  private int generatorMaxWindowsCount = 100;
  private boolean allInline = true;


  public void setUnitTestMode() {
     generatorMaxWindowsCount = 20;
  }

  public void setLocalMode() {
     generatorMaxWindowsCount = 20;
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

    this.generatorMaxWindowsCount = conf.getInt(P_generatorMaxWindowsCount, this.generatorMaxWindowsCount);
    this.allInline = conf.getBoolean(P_allInline, this.allInline);
  }

  private Operator getConsoleOperator(DAG b, String operatorName)
  {
    // output to HTTP server when specified in environment setting
    String serverAddr = System.getenv("MALHAR_AJAXSERVER_ADDRESS");
    if (serverAddr != null) {
      return b.addOperator(operatorName, HttpOutputModule.class)
              .setProperty(HttpOutputModule.P_RESOURCE_URL, "http://" + serverAddr + "/channel/mobile/" + operatorName);
    }
    return b.addOperator(operatorName, ConsoleOutputModule.class)
            //.setProperty(ConsoleOutputModule.P_DEBUG, "true")
            .setProperty(ConsoleOutputModule.P_STRING_FORMAT, operatorName + ": %s");
  }

  public Operator getSeedGenerator(String name, DAG b) {
    Operator oper = b.addOperator(name, LoadSeedGenerator.class);
    oper.setProperty(LoadSeedGenerator.KEY_STRING_SCHEMA, "false");
    oper.setProperty(LoadSeedGenerator.KEY_EMITKEY, "false");
    oper.setProperty(LoadSeedGenerator.KEY_KEYS, "x:0,500;y:0,500");
    oper.setProperty(LoadSeedGenerator.KEY_SEED_START, "9000000");
    oper.setProperty(LoadSeedGenerator.KEY_SEED_END    , "9999999");

    return oper;
  }

  public Operator getRandomGenerator(String name, DAG b) {
    Operator oper = b.addOperator(name, LoadRandomGenerator.class);
    oper.setProperty(LoadRandomGenerator.KEY_MAX_VALUE, "99");
    oper.setProperty(LoadRandomGenerator.KEY_MIN_VALUE, "0");
    oper.setProperty(LoadRandomGenerator.KEY_STRING_SCHEMA, "false");
    oper.setProperty("debugid", name);
    return oper;
  }

  public Operator getSeedClassifier(String name, DAG b) {
    Operator oper = b.addOperator(name, SeedClassifier.class);
    oper.setProperty(SeedClassifier.KEY_SEED_START, "9000000");
    oper.setProperty(SeedClassifier.KEY_SEED_END    , "9999999");
    oper.setProperty(SeedClassifier.KEY_IN_DATA1_CLASSIFIER, "x");
    oper.setProperty(SeedClassifier.KEY_IN_DATA2_CLASSIFIER, "y");
    oper.setProperty(SeedClassifier.KEY_STRING_SCHEMA, "false");
    return oper;
  }

  public Operator getTupleQueue(String name, DAG b) {
    Operator oper = b.addOperator(name, TupleQueue.class);
    oper.setProperty(TupleQueue.KEY_DEPTH, "5");

    return oper;
  }

  public Operator getInvertIndexMap(String name, DAG b) {
    return b.addOperator(name, InvertIndexMap.class);
  }

  public Operator getIncrementer(String name, DAG b) {
    Operator oper = b.addOperator(name, LoadIncrementer.class);
    oper.setProperty(LoadIncrementer.KEY_KEYS, "x,y");
    oper.setProperty(LoadIncrementer.KEY_DELTA, "1");
    oper.setProperty(LoadIncrementer.KEY_LIMITS, "0,500;0,500");
    return oper;
  }

  @Override
  public DAG getApplication(Configuration conf) {

    DAG dag = new DAG(conf);

    // Waiting for local server to be set up. For now I hardcoded the phones to be dumped
    // Operator phoneLocationQuery = dag.addOperator("phoneLocationQuery", HttpInputModule.class);
    // phoneLocationQuery.setProperty(HttpInputModule.P_RESOURCE_URL, "http://localhost:8080/channel/mobile/phoneLocationQuery");

    Operator seedGen = getSeedGenerator("seedGen", dag);
    Operator randomXGen = getRandomGenerator("xgen", dag);
    Operator randomYGen = getRandomGenerator("ygen", dag);
    Operator seedClassify = getSeedClassifier("seedclassify", dag);
    Operator incrementer = getIncrementer("incrementer", dag);
    // Operator tupleQueue = getTupleQueue("location_queue", dag);
    Operator indexMap = getInvertIndexMap("index_map", dag);
    Operator phoneconsole = getConsoleOperator(dag, "phoneLocationQueryResult");

    dag.addStream("seeddata", seedGen.getOutput(LoadSeedGenerator.OPORT_DATA), incrementer.getInput(LoadIncrementer.IPORT_SEED)).setInline(true);
    dag.addStream("xdata", randomXGen.getOutput(LoadRandomGenerator.OPORT_DATA), seedClassify.getInput(SeedClassifier.IPORT_IN_DATA1)).setInline(true);
    dag.addStream("ydata", randomYGen.getOutput(LoadRandomGenerator.OPORT_DATA), seedClassify.getInput(SeedClassifier.IPORT_IN_DATA2)).setInline(true);
    dag.addStream("incrdata", seedClassify.getOutput(SeedClassifier.OPORT_OUT_DATA), incrementer.getInput(LoadIncrementer.IPORT_INCREMENT)).setInline(true);
    dag.addStream("mobilelocation", incrementer.getOutput(LoadIncrementer.OPORT_DATA), indexMap.getInput(InvertIndexMap.IPORT_DATA)).setInline(true);
    //dag.addStream("mobilequery", phoneLocationQuery.getOutput(HttpInputModule.OUTPUT, indexMap.getInput(InvertIndexMap.IPORT_QUERY));
    dag.addStream("consoledata", indexMap.getOutput(InvertIndexMap.OPORT_CONSOLE), phoneconsole.getInput(HttpOutputModule.INPUT)).setInline(true);

    return dag;
  }
}

