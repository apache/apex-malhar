/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.mrmonitor;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import com.datatorrent.lib.io.PubSubWebSocketInputOperator;
import com.datatorrent.lib.io.PubSubWebSocketOutputOperator;

/**
 * <p>
 * MRDebuggerApplication class.
 * </p>
 *
 * @since 0.3.4
 */
@ApplicationAnnotation(name = "MRMonitoringDemo")
public class MRMonitoringApplication implements StreamingApplication
{

  private static final Logger logger = LoggerFactory.getLogger(MRMonitoringApplication.class);

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    String daemonAddress = dag.getValue(DAG.GATEWAY_CONNECT_ADDRESS);
    MRJobStatusOperator mrJobOperator = dag.addOperator("JobMonitor", new MRJobStatusOperator());
    URI uri = URI.create("ws://" + daemonAddress + "/pubsub");
    logger.info("WebSocket with daemon at {}", daemonAddress);

    PubSubWebSocketInputOperator wsIn = dag.addOperator("Query", new PubSubWebSocketInputOperator());
    wsIn.setUri(uri);

    MapToMRObjectOperator queryConverter = dag.addOperator("QueryConverter", new MapToMRObjectOperator());

    /**
     * This is used to emit the meta data about the job
     */
    PubSubWebSocketOutputOperator<Object> wsOut = dag.addOperator("JobOutput", new PubSubWebSocketOutputOperator<Object>());
    wsOut.setUri(uri);

    /**
     * This is used to emit the information of map tasks of the job
     */
    PubSubWebSocketOutputOperator<Object> wsMapOut = dag.addOperator("MapJob", new PubSubWebSocketOutputOperator<Object>());
    wsMapOut.setUri(uri);

    /**
     * This is used to emit the information of reduce tasks of the job
     */
    PubSubWebSocketOutputOperator<Object> wsReduceOut = dag.addOperator("ReduceJob", new PubSubWebSocketOutputOperator<Object>());
    wsReduceOut.setUri(uri);

    /**
     * This is used to emit the metric information of the job
     */
    PubSubWebSocketOutputOperator<Object> wsCounterOut = dag.addOperator("JobCounter", new PubSubWebSocketOutputOperator<Object>());
    wsCounterOut.setUri(uri);

    dag.addStream("QueryConversion", wsIn.outputPort, queryConverter.input);
    dag.addStream("QueryProcessing", queryConverter.output, mrJobOperator.input);
    dag.addStream("JobData", mrJobOperator.output, wsOut.input);
    dag.addStream("MapData", mrJobOperator.mapOutput, wsMapOut.input);
    dag.addStream("ReduceData", mrJobOperator.reduceOutput, wsReduceOut.input);
    dag.addStream("CounterData", mrJobOperator.counterOutput, wsCounterOut.input);
  }

}
