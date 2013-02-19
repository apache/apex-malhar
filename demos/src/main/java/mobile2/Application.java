/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package mobile2;

import com.google.common.collect.Range;
import com.google.common.collect.Ranges;
import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.demos.mobile.InvertIndexMapPhone;
import com.malhartech.lib.io.ConsoleOutputOperator;
import com.malhartech.lib.io.HttpInputOperator;
import com.malhartech.lib.io.HttpOutputOperator;
import com.malhartech.lib.testbench.EventIncrementer;
import com.malhartech.lib.testbench.RandomEventGenerator;
import com.malhartech.lib.testbench.SeedEventClassifier;
import com.malhartech.lib.testbench.SeedEventGenerator;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mobile Demo Application.<p>
 */
public class Application implements ApplicationFactory
{
  private static final Logger LOG = LoggerFactory.getLogger(Application.class);
  public static final String P_phoneRange = com.malhartech.demos.mobile.Application.class.getName() + ".phoneRange";
  private String ajaxServerAddr = null;
  private Range<Integer> phoneRange = Ranges.closed(9000000, 9999999);

  private void configure(Configuration conf)
  {

    this.ajaxServerAddr = System.getenv("MALHAR_AJAXSERVER_ADDRESS");
    LOG.debug(String.format("\n******************* Server address was %s", this.ajaxServerAddr));

    if (LAUNCHMODE_YARN.equals(conf.get(DAG.STRAM_LAUNCH_MODE))) {
      // settings only affect distributed mode
      conf.setIfUnset(DAG.STRAM_CONTAINER_MEMORY_MB.name(), "2048");
      conf.setIfUnset(DAG.STRAM_MASTER_MEMORY_MB.name(), "1024");
      conf.setIfUnset(DAG.STRAM_MAX_CONTAINERS.name(), "1");
    }
    else if (LAUNCHMODE_LOCAL.equals(conf.get(DAG.STRAM_LAUNCH_MODE))) {
    }

    String phoneRange = conf.get(P_phoneRange, null);
    if (phoneRange != null) {
      String[] tokens = phoneRange.split("-");
      if (tokens.length != 2) {
        throw new IllegalArgumentException("Invalid range: " + phoneRange);
      }
      this.phoneRange = Ranges.closed(Integer.parseInt(tokens[0]), Integer.parseInt(tokens[1]));
    }
    System.out.println("Phone range: " + this.phoneRange);
  }

  private ConsoleOutputOperator getConsoleOperator(DAG b, String name)
  {
    // output to HTTP server when specified in environment setting
    ConsoleOutputOperator oper = b.addOperator(name, new ConsoleOutputOperator());
    oper.setStringFormat(name + ": %s");
    return oper;
  }

  private HttpOutputOperator<HashMap<String, Object>> getHttpOutputNumberOperator(DAG b, String name)
  {
    // output to HTTP server when specified in environment setting
    String serverAddr =  this.ajaxServerAddr;
    HttpOutputOperator<HashMap<String, Object>> oper = b.addOperator(name, new HttpOutputOperator<HashMap<String, Object>>());
    URI u = URI.create("http://" + serverAddr + "/channel/mobile/" + name);
    oper.setResourceURL(u);
    return oper;
  }

  public RandomEventGenerator getRandomGenerator(String name, DAG b)
  {
    RandomEventGenerator oper = b.addOperator(name, RandomEventGenerator.class);
    oper.setMaxvalue(9999999);
    oper.setMinvalue(9990000);
    oper.setTuplesBlast(100000);
    oper.setTuplesBlastIntervalMillis(5);
    return oper;
  }

  public PhoneMovementGenerator getPhoneMovementGenerator(String name, DAG b)
  {
    PhoneMovementGenerator oper = b.addOperator(name, PhoneMovementGenerator.class);
    oper.setRange(20);
    oper.setThreshold(80);
    return oper;
  }
  @Override
  public DAG getApplication(Configuration conf)
  {

    DAG dag = new DAG(conf);
    configure(conf);

    RandomEventGenerator phones = getRandomGenerator("phonegen", dag);
    PhoneMovementGenerator movementgen = getPhoneMovementGenerator("pmove", dag);
    dag.addStream("phonedata", phones.integer_data, movementgen.data).setInline(true);

    if (this.ajaxServerAddr != null) {
      // TBD

    }
    else {
      // for testing purposes without server
      ConsoleOutputOperator phoneconsole = getConsoleOperator(dag, "phoneLocationQueryResult");
      dag.addStream("consoledata", movementgen.locations, phoneconsole.input).setInline(true);
    }
    return dag;
  }
}
