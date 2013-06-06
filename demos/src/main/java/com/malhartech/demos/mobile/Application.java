/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.demos.mobile;

import com.google.common.collect.Range;
import com.google.common.collect.Ranges;
import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DAG;
import com.malhartech.lib.io.*;
import com.malhartech.lib.testbench.RandomEventGenerator;

import java.net.URI;

import org.apache.commons.lang.StringUtils;
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
  private Range<Integer> phoneRange = Ranges.closed(9900000, 9999999);

  private void configure(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.STRAM_MAX_CONTAINERS, 1);
    if (LAUNCHMODE_YARN.equals(conf.get(DAG.STRAM_LAUNCH_MODE))) {
      // settings only affect distributed mode
      dag.getAttributes().attr(DAG.STRAM_CONTAINER_MEMORY_MB).setIfAbsent(2048);
      dag.getAttributes().attr(DAG.STRAM_MASTER_MEMORY_MB).setIfAbsent(1024);
      dag.getAttributes().attr(DAG.STRAM_MAX_CONTAINERS).setIfAbsent(1);
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

  @Override
  public void getApplication(DAG dag, Configuration conf)
  {
    configure(dag, conf);

    dag.setAttribute(DAG.STRAM_APPNAME, "MobileDevApplication");
    dag.setAttribute(DAG.STRAM_DEBUG, true);

    RandomEventGenerator phones = dag.addOperator("phonegen", RandomEventGenerator.class);
    phones.setMinvalue(this.phoneRange.lowerEndpoint());
    phones.setMaxvalue(this.phoneRange.upperEndpoint());
    phones.setTuplesBlast(1000);
    phones.setTuplesBlastIntervalMillis(5);

    PhoneMovementGenerator movementGen = dag.addOperator("pmove", PhoneMovementGenerator.class);
    movementGen.setRange(20);
    movementGen.setThreshold(80);
    dag.setAttribute(movementGen, OperatorContext.INITIAL_PARTITION_COUNT, 2);
    dag.setAttribute(movementGen, OperatorContext.PARTITION_TPS_MIN, 10000);
    dag.setAttribute(movementGen, OperatorContext.PARTITION_TPS_MAX, 50000);

    // default partitioning: first connected stream to movementGen will be partitioned
    dag.addStream("phonedata", phones.integer_data, movementGen.data).setInline(true);

    String daemonAddress = dag.attrValue(DAG.STRAM_DAEMON_ADDRESS, null);
    if (!StringUtils.isEmpty(daemonAddress)) {
      URI uri = URI.create("ws://" + daemonAddress + "/pubsub");
      LOG.info("WebSocket with daemon at: {}", daemonAddress);

      PubSubWebSocketOutputOperator<Object> wsOut = dag.addOperator("phoneLocationQueryResultWS", new PubSubWebSocketOutputOperator<Object>());
      wsOut.setUri(uri);
      wsOut.setTopic("demos.mobile.phoneLocationQueryResult");

      PubSubWebSocketInputOperator wsIn = dag.addOperator("phoneLocationQueryWS", new PubSubWebSocketInputOperator());
      wsIn.setUri(uri);
      wsIn.addTopic("demos.mobile.phoneLocationQuery");

      dag.addStream("consoledata", movementGen.locationQueryResult, wsOut.input).setInline(true);
      dag.addStream("query", wsIn.outputPort, movementGen.locationQuery);
    }
    else {
      // for testing purposes without server
      movementGen.phone_register.put("q1", 9994995);
      movementGen.phone_register.put("q3", 9996101);
      ConsoleOutputOperator out = dag.addOperator("phoneLocationQueryResult", new ConsoleOutputOperator());
      out.setStringFormat("phoneLocationQueryResult" + ": %s");
      dag.addStream("consoledata", movementGen.locationQueryResult, out.input).setInline(true);
    }

  }

}
