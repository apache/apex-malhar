/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.lastpricealert;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.api.Operator.InputPort;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.math.ChangeAlert;
import com.malhartech.lib.testbench.EventClassifierNumberToHashDouble;
import com.malhartech.lib.testbench.RandomEventGenerator;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;


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

  private <T> InputPort<HashMap<String, T>> getConsolePort(DAG b, String name)
  {
    ConsoleOutputOperator<HashMap<String, T>> oper = b.addOperator(name, new ConsoleOutputOperator<HashMap<String, T>>());
    oper.setStringFormat(name + ": %s");
    return oper.input;
  }

  public ChangeAlert<String,Double> getChangeAlertOperator(String name, DAG b)
  {
    ChangeAlert<String,Double> oper = b.addOperator(name, new ChangeAlert<String,Double>());
    oper.setPercentThreshold(5);
    return oper;
  }

   public RandomEventGenerator getRandomGenerator(String name, DAG b)
  {
    RandomEventGenerator oper = b.addOperator(name, new RandomEventGenerator());
    oper.setMaxvalue(1000);
    oper.setMinvalue(940);
    oper.setTuplesBlast(1000);
    oper.setTuplesBlastIntervalMillis(50);
    return oper;
  }

  public EventClassifierNumberToHashDouble<Integer> getEventClassifier(String name, DAG b)
  {
    EventClassifierNumberToHashDouble<Integer> oper = b.addOperator(name, new EventClassifierNumberToHashDouble<Integer>());
    oper.setKey("a");
    return oper;
  }


  @Override
  public DAG getApplication(Configuration conf)
  {

    configure(conf);
    DAG dag = new DAG(conf);

    RandomEventGenerator rGen = getRandomGenerator("randomgen", dag);
    EventClassifierNumberToHashDouble<Integer> hGen = getEventClassifier("hgen", dag);
    ChangeAlert<String,Double> alert = getChangeAlertOperator("alert", dag);
    // Need an operator that converts Integer from rGen to {"a"=val} a String,Double
    // That is to be input to alert, and that should directly write to console

    dag.addStream("randomdata", rGen.integer_data, hGen.event).setInline(true);
    dag.addStream("pricedata", hGen.data, alert.data).setInline(true);

    InputPort<HashMap<String, HashMap<Double,Double>>> alertConsole = getConsolePort(dag, "alertConsole");
    dag.addStream("consolestream", alert.alert, alertConsole).setInline(allInline);
    return dag;
  }
}
