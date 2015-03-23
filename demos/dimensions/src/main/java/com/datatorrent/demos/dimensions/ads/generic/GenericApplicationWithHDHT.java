/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.dimensions.ads.generic;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.contrib.kafka.KafkaJsonEncoder;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.contrib.kafka.SimpleKafkaConsumer;
import com.datatorrent.demos.dimensions.ads.ApplicationWithHDHT;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;


/**
 * An AdsDimensionsDemo run with HDHT
 *
 * Example of configuration
 <pre>
 {@code
 <property>
 <name>dt.application.AdsDimensionsWithHDSDemo.class</name>
 <value>com.datatorrent.demos.adsdimension.ApplicationWithHDS</value>
 </property>

 <property>
 <name>dt.application.AdsDimensionsWithHDSDemo.attr.containerMemoryMB</name>
 <value>8192</value>
 </property>

 <property>
 <name>dt.application.AdsDimensionsWithHDSDemo.attr.containerJvmOpts</name>
 <value>-Xmx6g -server -Dlog4j.debug=true -Xloggc:&lt;LOG_DIR&gt;/gc.log -verbose:gc -XX:+PrintGCDateStamps</value>
 </property>

 <property>
 <name>dt.application.AdsDimensionsWithHDSDemo.port.*.attr.QUEUE_CAPACITY</name>
 <value>32000</value>
 </property>

 <property>
 <name>dt.operator.InputGenerator.attr.PARTITIONER</name>
 <value>com.datatorrent.lib.partitioner.StatelessPartitioner:8</value>
 </property>

 <property>
 <name>dt.operator.DimensionsComputation.attr.APPLICATION_WINDOW_COUNT</name>
 <value>4</value>
 </property>

 <property>
 <name>dt.operator.DimensionsComputation.port.data.attr.PARTITION_PARALLEL</name>
 <value>true</value>
 </property>

 <property>
 <name>dt.operator.HDSOut.attr.PARTITIONER</name>
 <value>com.datatorrent.lib.partitioner.StatelessPartitioner:4</value>
 </property>

 <property>
 <name>dt.operator.HDSOut.fileStore.basePath</name>
 <value>AdsDimensionWithHDS</value>
 </property>

 <property>
 <name>dt.operator.Query.topic</name>
 <value>HDSQuery</value>
 </property>

 <property>
 <name>dt.operator.QueryResult.topic</name>
 <value>HDSQueryResult</value>
 </property>

 <property>
 <name>dt.operator.Query.brokerSet</name>
 <value>localhost:9092</value>
 </property>

 <property>
 <name>dt.operator.QueryResult.prop.configProperties(metadata.broker.list)</name>
 <value>localhost:9092</value>
 </property>

 }
 </pre>
 *
 * @since 2.0.0
 */
@ApplicationAnnotation(name=ApplicationWithHDHT.APP_NAME)
public class GenericApplicationWithHDHT implements StreamingApplication
{
  public static final String APP_NAME = "AdsDimensionsDemoWithHDHTtest";
  public static final String PROP_USE_WEBSOCKETS = "dt.application." + APP_NAME + ".useWebSockets";

  public static final String EVENT_SCHEMA = "adsGenericDataSchema.json";
  public static final String DIMENSIONAL_SCHEMA = "adsGenericDataSchema.json";

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    GenericInputItemGenerator input = dag.addOperator("InputGenerator", GenericInputItemGenerator.class);
    GenericAdsDimensionComputation dimensions = dag.addOperator("DimensionsComputation", new GenericAdsDimensionComputation());
    dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 4);
    GenericAdsDimensionStore store = dag.addOperator("Store", GenericAdsDimensionStore.class);
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    store.setFileStore(hdsFile);
    dag.setAttribute(store, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator< MutableLong >());


    StringWriter eventWriter = new StringWriter();
    try {
      IOUtils.copy(GenericApplicationWithHDHT.class.getClassLoader().getResourceAsStream(EVENT_SCHEMA),
                   eventWriter);
    }
    catch(IOException ex) {
      throw new RuntimeException(ex);
    }
    String eventSchema = eventWriter.toString();

    StringWriter dimensionalWriter = new StringWriter();
    try {
      IOUtils.copy(GenericApplicationWithHDHT.class.getClassLoader().getResourceAsStream(DIMENSIONAL_SCHEMA),
                   dimensionalWriter);
    }
    catch(IOException ex) {
      throw new RuntimeException(ex);
    }
    String dimensionalSchema = dimensionalWriter.toString();

    dimensions.setEventSchemaJSON(eventSchema);
    store.setEventSchemaJSON(eventSchema);
    store.setDimensionalSchemaJSON(dimensionalSchema);

    Operator.OutputPort<String> queryPort;
    Operator.InputPort<String> queryResultPort;
    if (conf.getBoolean(PROP_USE_WEBSOCKETS,  false)) {
      String gatewayAddress = dag.getValue(DAG.GATEWAY_CONNECT_ADDRESS);
      URI uri = URI.create("ws://" + gatewayAddress + "/pubsub");
      //LOG.info("WebSocket with gateway at: {}", gatewayAddress);
      PubSubWebSocketAppDataQuery wsIn = dag.addOperator("Query", new PubSubWebSocketAppDataQuery());
      wsIn.setUri(uri);
      queryPort = wsIn.outputPort;
      PubSubWebSocketAppDataResult wsOut = dag.addOperator("QueryResult", new PubSubWebSocketAppDataResult());
      wsOut.setUri(uri);
      queryResultPort = wsOut.input;
    } else {
      KafkaSinglePortStringInputOperator queries = dag.addOperator("Query", new KafkaSinglePortStringInputOperator());
      queries.setConsumer(new SimpleKafkaConsumer());
      queryPort = queries.outputPort;
      KafkaSinglePortOutputOperator<String, String> queryResult = dag.addOperator("QueryResult", new KafkaSinglePortOutputOperator<String, String>());
      queryResult.getConfigProperties().put("serializer.class", KafkaJsonEncoder.class.getName());
      queryResultPort = queryResult.inputPort;
    }

    dag.addStream("InputStream", input.outputPort, dimensions.inputEvent).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("DimensionalData", dimensions.aggregateOutput, store.input);
    dag.addStream("Query", queryPort, store.query);
    dag.addStream("QueryResult", store.queryResult, queryResultPort);
  }
}

