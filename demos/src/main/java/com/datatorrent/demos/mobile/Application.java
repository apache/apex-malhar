/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
 * limitations under the License.
 */
package com.datatorrent.demos.mobile;

import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.DAGContext;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.PubSubWebSocketInputOperator;
import com.datatorrent.lib.io.PubSubWebSocketOutputOperator;
import com.datatorrent.lib.testbench.RandomEventGenerator;
import com.google.common.collect.Range;
import com.google.common.collect.Ranges;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Random;

/**
 * Mobile Demo Application:
 * <p>
 * This demo simulates large number of cell phones in the range of 40K to 200K
 * and tracks a given cell number across cell towers. It also displays the changing locations of the cell number on a google map.
 *
 * This demo demonstrates the scalability feature of Datatorrent platform.
 * It showcases the ability of the platform to scale up and down as the phone numbers generated increase and decrease respectively.
 * If the tuples processed per second by the pmove operator increase beyond 30,000, more partitions of the pmove operator gets deployed until
 * each of the partition processes around 10000 to 30000 tuples per second.
 * If the tuples processed per second drops below 10,000, the platform merges the operators until the partition count drops down to the original.
 * The load can be varied using the tuplesBlast property.
 * If the tuplesBlast is set to 200, 40K cell phones are generated.
 * If the tuplesBlast is set to 1000, 200K cell phones are generated.
 * The tuplesBlast property can be set using dtcli command: 'set-operator-property pmove tuplesBlast 1000'.
 *
 *
 * The specs are as such<br>
 * Depending on the tuplesBlast property, large number of cell phone numbers are generated.
 * They jump a cell tower frequently. Sometimes
 * within a second sometimes in 10 seconds. The aim is to demonstrate the
 * following abilities<br>
 * <ul>
 * <li>Entering query dynamically: The phone numbers are added to locate its gps
 * in run time.</li>
 * <li>Changing functionality dynamically: The load is changed by making
 * functional changes on the load generator operator (phonegen)(</li>
 * <li>Auto Scale up/Down with load: Operator pmove increases and decreases
 * partitions as per load</li>
 * <li></li>
 * </ul>
 *
 * Refer to demos/docs/MobileDemo.md for more information.
 *
 * <p>
 *
 * Running Java Test or Main app in IDE:
 *
 * <pre>
 * LocalMode.runApp(new Application(), 600000); // 10 min run
 * </pre>
 *
 * Run Success : <br>
 * For successful deployment and run, user should see following output on
 * console: <br>
 *
 * <pre>
 * phoneLocationQueryResult: {phone=5556101, location=(5,9), queryId=q3}
 * phoneLocationQueryResult: {phone=5554995, location=(10,4), queryId=q1}
 * phoneLocationQueryResult: {phone=5556101, location=(5,9), queryId=q3}
 * phoneLocationQueryResult: {phone=5554995, location=(10,4), queryId=q1}
 * phoneLocationQueryResult: {phone=5554995, location=(10,5), queryId=q1}
 * phoneLocationQueryResult: {phone=5556101, location=(5,9), queryId=q3}
 * phoneLocationQueryResult: {phone=5554995, location=(9,5), queryId=q1}
 * phoneLocationQueryResult: {phone=5556101, location=(5,9), queryId=q3}
 * phoneLocationQueryResult: {phone=5556101, location=(5,9), queryId=q3}
 * phoneLocationQueryResult: {phone=5554995, location=(9,5), queryId=q1}
 * phoneLocationQueryResult: {phone=5554995, location=(9,5), queryId=q1}
 * phoneLocationQueryResult: {phone=5556101, location=(5,9), queryId=q3}
 * </pre>
 *
 *  * <b>Application DAG : </b><br>
 * <img src="doc-files/mobile.png" width=600px > <br>
 *
 * @since 0.3.2
 */
@ApplicationAnnotation(name="MobileApplication")
public class Application implements StreamingApplication
{
  private static final Logger LOG = LoggerFactory.getLogger(Application.class);
  public static final String P_phoneRange = com.datatorrent.demos.mobile.Application.class.getName() + ".phoneRange";
  public static final String TOTAL_SEED_NOS = com.datatorrent.demos.mobile.Application.class.getName() + ".totalSeedNumbers";

  private Range<Integer> phoneRange = Ranges.closed(5550000, 5559999);

  private void configure(DAG dag, Configuration conf)
  {
    //dag.setAttribute(DAG.CONTAINERS_MAX_COUNT, 1);
    if (LAUNCHMODE_YARN.equals(conf.get(DAG.LAUNCH_MODE))) {
      // settings only affect distributed mode
      AttributeMap attributes = dag.getAttributes();
      if (attributes.get(DAGContext.CONTAINER_MEMORY_MB) == null) {
        attributes.put(DAGContext.CONTAINER_MEMORY_MB, 2048);
      }
      if (attributes.get(DAGContext.MASTER_MEMORY_MB) == null) {
        attributes.put(DAGContext.MASTER_MEMORY_MB, 1024);
      }
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

    dag.setAttribute(DAG.APPLICATION_NAME, "MobileApplication");
    dag.setAttribute(DAG.DEBUG, true);

    RandomEventGenerator phones = dag.addOperator("phonegen", RandomEventGenerator.class);
    phones.setMinvalue(this.phoneRange.lowerEndpoint());
    phones.setMaxvalue(this.phoneRange.upperEndpoint());
    phones.setTuplesBlast(200);
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

    // generate seed numbers
    Random random = new Random();
    int maxPhone = phoneRange.upperEndpoint() - 5550000;
    int phonesToDisplay = conf.getInt(TOTAL_SEED_NOS,10);

    for (int i = phonesToDisplay; i-- > 0; ) {
      int phoneNo = 5550000 + random.nextInt(maxPhone + 1);
      LOG.info("seed no: " + phoneNo);
      movementGen.phone_register.add(phoneNo);
    }

    // done generating data
    LOG.info("Finished generating seed data.");

    String gatewayAddress = dag.getValue(DAG.GATEWAY_CONNECT_ADDRESS);
    if (!StringUtils.isEmpty(gatewayAddress)) {
      URI uri = URI.create("ws://" + gatewayAddress + "/pubsub");
      LOG.info("WebSocket with gateway at: {}", gatewayAddress);

      PubSubWebSocketOutputOperator<Object> wsOut = dag.addOperator("phoneLocationQueryResultWS", new PubSubWebSocketOutputOperator<Object>());
      wsOut.setUri(uri);
      wsOut.setTopic("demos.mobile.phoneLocationQueryResult");

      PubSubWebSocketInputOperator wsIn = dag.addOperator("phoneLocationQueryWS", new PubSubWebSocketInputOperator());
      wsIn.setUri(uri);
      wsIn.addTopic("demos.mobile.phoneLocationQuery");

      dag.addStream("consoledata", movementGen.locationQueryResult, wsOut.input);
      dag.addStream("query", wsIn.outputPort, movementGen.phoneQuery);
    }
    else {
      // for testing purposes without server
      movementGen.phone_register.add(5554995);
      movementGen.phone_register.add(5556101);
      ConsoleOutputOperator out = dag.addOperator("phoneLocationQueryResult", new ConsoleOutputOperator());
      out.setStringFormat("phoneLocationQueryResult" + ": %s");
      dag.addStream("consoledata", movementGen.locationQueryResult, out.input).setLocality(Locality.CONTAINER_LOCAL);
    }
  }

}
