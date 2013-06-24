/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. See accompanying LICENSE file.
 */
package com.datatorrent.contrib.summit.mobile;

import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.lib.io.*;
import com.datatorrent.lib.testbench.RandomEventGenerator;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;

import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Mobile Demo Application.<p>
 */
public class Application implements StreamingApplication
{
  private static final Logger LOG = LoggerFactory.getLogger(Application.class);
  public static final String P_phoneRange = com.datatorrent.contrib.summit.mobile.Application.class.getName() + ".phoneRange";
  private Range<Integer> phoneRange = Ranges.closed(9900000, 9999999);

  private void configure(DAG dag, Configuration conf)
  {
    //dag.setAttribute(DAG.CONTAINERS_MAX_COUNT, 1);
    if (LAUNCHMODE_YARN.equals(conf.get(DAG.LAUNCH_MODE))) {
      // settings only affect distributed mode
      dag.getAttributes().attr(DAG.CONTAINER_MEMORY_MB).setIfAbsent(2048);
      dag.getAttributes().attr(DAG.MASTER_MEMORY_MB).setIfAbsent(1024);
      //dag.getAttributes().attr(DAG.CONTAINERS_MAX_COUNT).setIfAbsent(1);
    }
    else if (LAUNCHMODE_LOCAL.equals(conf.get(DAG.LAUNCH_MODE))) {
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
  public void populateDAG(DAG dag, Configuration conf)
  {
    configure(dag, conf);

    dag.setAttribute(DAG.APPLICATION_NAME, "MobileLocatorApplication");
    dag.setAttribute(DAG.DEBUG, true);

    RandomEventGenerator phones = dag.addOperator("phonegen", RandomEventGenerator.class);
    phones.setMinvalue(this.phoneRange.lowerEndpoint());
    phones.setMaxvalue(this.phoneRange.upperEndpoint());
    phones.setTuplesBlast(1000);
    phones.setTuplesBlastIntervalMillis(5);
    dag.setOutputPortAttribute(phones.integer_data, PortContext.QUEUE_CAPACITY, 32 * 1024);

    PhoneMovementGenerator movementGen = dag.addOperator("pmove", PhoneMovementGenerator.class);
    movementGen.setRange(20);
    movementGen.setThreshold(80);
    dag.setAttribute(movementGen, OperatorContext.INITIAL_PARTITION_COUNT, 2);
    dag.setAttribute(movementGen, OperatorContext.PARTITION_TPS_MIN, 10000);
    dag.setAttribute(movementGen, OperatorContext.PARTITION_TPS_MAX, 30000);
    dag.setInputPortAttribute(movementGen.data, PortContext.QUEUE_CAPACITY, 32 * 1024);

    // default partitioning: first connected stream to movementGen will be partitioned
    dag.addStream("phonedata", phones.integer_data, movementGen.data);

    String daemonAddress = dag.attrValue(DAG.DAEMON_ADDRESS, null);
    if (!StringUtils.isEmpty(daemonAddress)) {
      URI uri = URI.create("ws://" + daemonAddress + "/pubsub");
      LOG.info("WebSocket with daemon at: {}", daemonAddress);

      PubSubWebSocketOutputOperator<Object> wsOut = dag.addOperator("phoneLocationQueryResultWS", new PubSubWebSocketOutputOperator<Object>());
      wsOut.setUri(uri);
      wsOut.setTopic("demos.mobile.phoneLocationQueryResult");

      PubSubWebSocketInputOperator wsIn = dag.addOperator("phoneLocationQueryWS", new PubSubWebSocketInputOperator());
      wsIn.setUri(uri);
      wsIn.addTopic("demos.mobile.phoneLocationQuery");

      dag.addStream("consoledata", movementGen.locationQueryResult, wsOut.input);
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
