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

import java.net.URI;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Range;
import com.google.common.collect.Ranges;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.DAGContext;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.io.PubSubWebSocketInputOperator;
import com.datatorrent.lib.io.PubSubWebSocketOutputOperator;
import com.datatorrent.lib.io.SmtpOutputOperator;
import com.datatorrent.lib.testbench.RandomEventGenerator;
import com.datatorrent.lib.util.AlertEscalationOperator;

/**
 * Mobile Demo Application with Alert.<p>
 *
 * @since 0.3.2
 */
@ApplicationAnnotation(name="MobileApplicationWithAlert")
public class ApplicationAlert implements StreamingApplication
{
  private static final Logger LOG = LoggerFactory.getLogger(ApplicationAlert.class);
  public static final String P_phoneRange = com.datatorrent.demos.mobile.Application.class.getName() + ".phoneRange";
  private Range<Integer> phoneRange = Ranges.closed(9900000, 9999999);

  private void configure(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.CONTAINERS_MAX_COUNT, 1);
    if (LAUNCHMODE_YARN.equals(conf.get(DAG.LAUNCH_MODE))) {
      // settings only affect distributed mode
      // settings only affect distributed mode
      AttributeMap attributes = dag.getAttributes();
      if (attributes.get(DAGContext.CONTAINER_MEMORY_MB) == null) {
        attributes.put(DAGContext.CONTAINER_MEMORY_MB, 2048);
      }
      if (attributes.get(DAGContext.MASTER_MEMORY_MB) == null) {
        attributes.put(DAGContext.MASTER_MEMORY_MB, 1024);
      }
      if (attributes.get(DAGContext.CONTAINERS_MAX_COUNT) == null) {
        attributes.put(DAGContext.CONTAINERS_MAX_COUNT, 1);
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
    dag.setAttribute(DAG.APPLICATION_NAME, "MobileAlertApplication");
    dag.setAttribute(DAG.DEBUG, true);

    RandomEventGenerator phones = dag.addOperator("phonegen", RandomEventGenerator.class);
    phones.setMinvalue(this.phoneRange.lowerEndpoint());
    phones.setMaxvalue(this.phoneRange.upperEndpoint());
    phones.setTuplesBlast(1000);
    phones.setTuplesBlastIntervalMillis(5);

    PhoneMovementGenerator movementgen = dag.addOperator("pmove", PhoneMovementGenerator.class);
    movementgen.setRange(20);
    movementgen.setThreshold(80);
    dag.setAttribute(movementgen, OperatorContext.INITIAL_PARTITION_COUNT, 2);
    dag.setAttribute(movementgen, OperatorContext.PARTITION_TPS_MIN, 10000);
    dag.setAttribute(movementgen, OperatorContext.PARTITION_TPS_MAX, 50000);

    AlertEscalationOperator alertOper = dag.addOperator("palert", AlertEscalationOperator.class);
    alertOper.setAlertInterval(10000);
    alertOper.setActivated(false);

    dag.addStream("phonedata", phones.integer_data, movementgen.data).setLocality(Locality.CONTAINER_LOCAL);

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

      dag.addStream("consoledata", movementgen.locationQueryResult, wsOut.input, alertOper.in).setLocality(Locality.CONTAINER_LOCAL);
      dag.addStream("query", wsIn.outputPort, movementgen.phoneQuery);
    }
    else { // If no ajax, need to do phone seeding
      movementgen.phone_register.add(9996101);
      movementgen.phone_register.add(9994995);

      ConsoleOutputOperator console = dag.addOperator("phoneLocationQueryResult", new ConsoleOutputOperator());
      console.setStringFormat("result: %s");
      dag.addStream("consoledata", movementgen.locationQueryResult, console.input, alertOper.in);
    }

    SmtpOutputOperator mailOper = dag.addOperator("mail", new SmtpOutputOperator());
    mailOper.setFrom("jenkins@datatorrent.com");
    mailOper.addRecipient(SmtpOutputOperator.RecipientType.TO, "jenkins@datatorrent.com");
    mailOper.setContent("Phone Location: {}\nThis is an auto-generated message. Do not reply.");
    mailOper.setSubject("Update New Location");
    mailOper.setSmtpHost("secure.emailsrvr.com");
    mailOper.setSmtpPort(465);
    mailOper.setSmtpUserName("jenkins@datatorrent.com");
    mailOper.setSmtpPassword("Testing1");
    mailOper.setUseSsl(true);

    dag.addStream("alert_mail", alertOper.alert, mailOper.input).setLocality(Locality.CONTAINER_LOCAL);
  }
}
