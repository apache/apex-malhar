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
import com.malhartech.lib.math.SumValue;
import com.malhartech.lib.stream.DevNullCounter;
import com.malhartech.lib.testbench.EventClassifierNumberToHashDouble;
import com.malhartech.lib.testbench.RandomEventGenerator;
import com.malhartech.lib.testbench.ThroughputCounter;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;


/**
 * Example of application configuration for a last price demo<p>
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
      conf.setIfUnset(DAG.STRAM_CONTAINER_MEMORY_MB.name(), "2048");
      conf.setIfUnset(DAG.STRAM_MASTER_MEMORY_MB.name(), "1024");
      conf.setIfUnset(DAG.STRAM_MAX_CONTAINERS.name(), "1");
    }
    else if (LAUNCHMODE_LOCAL.equals(conf.get(DAG.STRAM_LAUNCH_MODE))) {
      setLocalMode();
    }

    this.generatorVTuplesBlast = conf.getInt(P_generatorVTuplesBlast, this.generatorVTuplesBlast);
    this.generatorMaxWindowsCount = conf.getInt(P_generatorMaxWindowsCount, this.generatorMaxWindowsCount);
    this.allInline = conf.getBoolean(P_allInline, this.allInline);

  }

  private InputPort<Object> getConsolePort(DAG b, String name)
  {
    ConsoleOutputOperator oper = b.addOperator(name, new ConsoleOutputOperator());
    oper.setStringFormat(name + ": %s");
    return oper.input;
  }

  public ChangeAlert<String,Double> getChangeAlertOperator(String name, DAG b)
  {
    ChangeAlert<String,Double> oper = b.addOperator(name, new ChangeAlert<String,Double>());
    oper.setPercentThreshold(2);
    String [] filters = new String[300];
    for (int i = 0; i < 300; i++) {
      String key = "a";
      Integer ival = i;
      key += ival.toString();
      filters[i] = key;
    }
    oper.setFilterBy(filters);
    oper.setInverse(false);
    return oper;
  }

   public RandomEventGenerator getRandomGenerator(String name, DAG b)
  {
    RandomEventGenerator oper = b.addOperator(name, new RandomEventGenerator());
    oper.setMaxvalue(1000);
    oper.setMinvalue(960);
    oper.setTuplesBlast(1000000);
    oper.setTuplesBlastIntervalMillis(50);
    return oper;
  }

  public EventClassifierNumberToHashDouble<Integer> getEventClassifier(String name, DAG b)
  {
    EventClassifierNumberToHashDouble<Integer> oper = b.addOperator(name, new EventClassifierNumberToHashDouble<Integer>());
    // have 1470 keys
    // Watch list of only 300
    oper.setKey("a");
    oper.setSeedstart(0);
    oper.setSeedend(1500);
    return oper;
  }

  public DevNullCounter<HashMap<String, HashMap<Double,Double>>> getDevNullOperator(String name, DAG b)
  {
    DevNullCounter<HashMap<String, HashMap<Double,Double>>> oper = b.addOperator(name, new DevNullCounter<HashMap<String, HashMap<Double,Double>>>());
    oper.setRollingwindowcount(10);
    oper.setDebug(true);
    return oper;
  }

  public SumValue<Integer> getSumValue(String name, DAG b)
  {
    return b.addOperator(name, new SumValue<Integer>());
  }

  public ThroughputCounter<String, Double> getThroughputCounter(String name, DAG b)
  {
    ThroughputCounter<String, Double> oper = b.addOperator(name, new ThroughputCounter<String, Double>());
    oper.setRollingWindowCount(5);
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
    DevNullCounter<HashMap<String, HashMap<Double,Double>>> onull = getDevNullOperator("null", dag);

    SumValue<Integer> scount = getSumValue("count", dag);
    EventClassifierNumberToHashDouble<Integer> hGentput = getEventClassifier("hgentput", dag);
    ThroughputCounter<String, Double> toper = getThroughputCounter("tcount", dag);

    InputPort<Object> alertconsole = getConsolePort(dag, "throughputConsole");

    // Need an operator that converts Integer from rGen to {"a"=val} a String,Double
    // That is to be input to alert, and that should directly write to console
    // RandomGen -> Insert key -> PriceChange -> DevNull

    dag.addStream("randomdata", rGen.integer_data, hGen.event, scount.data).setInline(true);
    dag.addStream("pricedata", hGen.data, alert.data).setInline(true);
    dag.addStream("nullstream", alert.alert, onull.data).setInline(allInline);

    // ThroughputCounter and console are no longer required as Stram stats shows the throughtput
    // scount.count gives the count of number of tuples in that window
    // This number is streamed into a ThroughputCounter and logged on alertconsole (stdout of IDE)
    dag.addStream("countstream", scount.count, hGentput.event).setInline(allInline);
    dag.addStream("tcountstream", hGentput.data, toper.data).setInline(allInline);
    dag.addStream("consolestream", toper.count, alertconsole).setInline(allInline);

    return dag;
  }
}
