/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.mobile;

import org.apache.hadoop.conf.Configuration;

import com.malhartech.dag.ApplicationFactory;
import com.malhartech.dag.DAG;
import com.malhartech.dag.DAG.Operator;
import com.malhartech.lib.io.ConsoleOutputModule;
import com.malhartech.lib.io.HttpInputModule;
import com.malhartech.lib.io.HttpOutputModule;
import com.malhartech.lib.testbench.LoadRandomGenerator;
import com.malhartech.lib.testbench.LoadSeedGenerator;
import com.malhartech.lib.testbench.SeedClassifier;


/**
 * Example of application configuration in Java using {@link com.malhartech.stram.conf.NewDAGBuilder}.<p>
 */

public class Application implements ApplicationFactory {


  public void setUnitTestMode() {
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

  public Operator getSeedGenerator(String name, DAG b) {
    Operator oper = b.addOperator(name, LoadSeedGenerator.class);
    oper.setProperty(LoadSeedGenerator.KEY_STRING_SCHEMA, "false");
    oper.setProperty(LoadSeedGenerator.KEY_EMITKEY, "false");
    oper.setProperty(LoadSeedGenerator.KEY_KEYS, "x,y");
    oper.setProperty(LoadSeedGenerator.KEY_SEED_START, "8000000");
    oper.setProperty(LoadSeedGenerator.KEY_SEED_END    , "9999999");

    return oper;
  }

  public Operator getRandomGenerator(String name, DAG b) {
    Operator oper = b.addOperator(name, LoadRandomGenerator.class);
    oper.setProperty(LoadRandomGenerator.KEY_MAX_VALUE, "99");
    oper.setProperty(LoadRandomGenerator.KEY_MIN_VALUE, "0");
    oper.setProperty(LoadRandomGenerator.KEY_TUPLES_BLAST, "1000");
    oper.setProperty(LoadRandomGenerator.KEY_STRING_SCHEMA, "false");
    return oper;
  }

  public Operator getSeedClassifier(String name, DAG b) {
    Operator oper = b.addOperator(name, SeedClassifier.class);
    oper.setProperty(SeedClassifier.KEY_SEED_END    , "9999999");
    oper.setProperty(SeedClassifier.KEY_SEED_START, "8000000");
    oper.setProperty(SeedClassifier.KEY_IN_DATA1_CLASSIFIER, "x");
    oper.setProperty(SeedClassifier.KEY_IN_DATA2_CLASSIFIER, "y");
    oper.setProperty(SeedClassifier.KEY_STRING_SCHEMA, "false");
    return oper;
  }

  @Override
  public DAG getApplication(Configuration conf) {

    DAG dag = new DAG(conf);

    Operator phoneLocationQuery = dag.addOperator("phoneLocationQuery", HttpInputModule.class);
    phoneLocationQuery.setProperty(HttpInputModule.P_RESOURCE_URL, "http://localhost:8080/channel/mobile/phoneLocationQuery");

    Operator seedGen = getSeedGenerator("seedGen", dag);
    Operator randomXGen = getRandomGenerator("xgen", dag);
    Operator randomYGen = getRandomGenerator("ygen", dag);
    Operator seedClassify = getSeedClassifier("seedclassify", dag);


    //dag.addStream("views", viewGen.getOutput(LoadGenerator.OPORT_DATA), adviews.getInput(LoadClassifier.IPORT_IN_DATA)).setInline(true);

    Operator viewcountconsole = getConsoleOperator(dag, "viewCountConsole");

    //dag.addStream("revenuedata", revenue.getOutput(ArithmeticSum.OPORT_SUM), margin.getInput(ArithmeticMargin.IPORT_DENOMINATOR), revconsole.getInput(ConsoleOutputModule.INPUT)).setInline(true);

    return dag;
  }
}

