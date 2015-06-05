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
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.demos.dimensions.ads.InputItemGenerator;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.dimensions.DimensionsComputationFlexibleSingleSchemaPOJO;
import com.datatorrent.lib.dimensions.DimensionsComputationUnifierImpl;
import com.datatorrent.lib.dimensions.DimensionsEvent.Aggregate;
import com.datatorrent.lib.dimensions.DimensionsEvent.InputEvent;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import java.net.URI;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Map;

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
@ApplicationAnnotation(name=AdsDimensionsDemo.APP_NAME)
public class AdsDimensionsDemo implements StreamingApplication
{
  public static final String APP_NAME = "AdsDimensionsDemoGeneric";
  public static final String EVENT_SCHEMA = "adsGenericEventSchema.json";

  public String appName = APP_NAME;
  public String eventSchemaLocation = EVENT_SCHEMA;
  public List<Object> advertisers;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    String propStorePath = "dt.application." + appName + ".operator.Store.fileStore.basePathPrefix";

    //Declare operators

    InputItemGenerator input = dag.addOperator("InputGenerator", InputItemGenerator.class);
    input.advertiserName = advertisers;
    DimensionsComputationFlexibleSingleSchemaPOJO dimensions = dag.addOperator("DimensionsComputation", DimensionsComputationFlexibleSingleSchemaPOJO.class);
    dimensions.setUnifier(new DimensionsComputationUnifierImpl<InputEvent, Aggregate>());
    dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 4);
    AppDataSingleSchemaDimensionStoreHDHT store = dag.addOperator("Store", AppDataSingleSchemaDimensionStoreHDHT.class);

    //Set operator properties

    //Set input properties
    String eventSchema = SchemaUtils.jarResourceFileToString(eventSchemaLocation);
    input.setEventSchemaJSON(eventSchema);

    Map<String, String> keyToExpression = Maps.newHashMap();
    keyToExpression.put("publisher", "getPublisher()");
    keyToExpression.put("advertiser", "getAdvertiser()");
    keyToExpression.put("location", "getLocation()");
    keyToExpression.put("time", "getTime()");

    Map<String, String> aggregateToExpression = Maps.newHashMap();
    aggregateToExpression.put("cost", "getCost()");
    aggregateToExpression.put("revenue", "getRevenue()");
    aggregateToExpression.put("impressions", "getImpressions()");
    aggregateToExpression.put("clicks", "getClicks()");

    dimensions.setKeyToExpression(keyToExpression);
    dimensions.setAggregateToExpression(aggregateToExpression);
    dimensions.setEventSchemaJSON(eventSchema);

    //Set store properties
    String basePath = Preconditions.checkNotNull(conf.get(propStorePath),
                                                 "a base path should be specified in the properties.xml");
    TFileImpl hdsFile = new TFileImpl.DTFileImpl();
    System.out.println(dag.getAttributes().get(DAG.APPLICATION_ID));
    basePath += Path.SEPARATOR + System.currentTimeMillis();
    hdsFile.setBasePath(basePath);
    System.out.println("Setting basePath " + basePath);
    store.setFileStore(hdsFile);
    store.getAppDataFormatter().setContinuousFormatString("#.00");
    store.setEventSchemaJSON(eventSchema);

    //Set pubsub properties
    Operator.OutputPort<String> queryPort;
    Operator.InputPort<String> queryResultPort;

    String gatewayAddress = dag.getValue(DAG.GATEWAY_CONNECT_ADDRESS);
    URI uri = URI.create("ws://" + gatewayAddress + "/pubsub");
    //LOG.info("WebSocket with gateway at: {}", gatewayAddress);
    PubSubWebSocketAppDataQuery wsIn = dag.addOperator("Query", new PubSubWebSocketAppDataQuery());
    wsIn.setUri(uri);
    queryPort = wsIn.outputPort;
    PubSubWebSocketAppDataResult wsOut = dag.addOperator("QueryResult", new PubSubWebSocketAppDataResult());
    wsOut.setUri(uri);
    queryResultPort = wsOut.input;

    //Set remaining dag options

    dag.setAttribute(store, Context.OperatorContext.COUNTERS_AGGREGATOR, new BasicCounters.LongAggregator<MutableLong>());

    dag.addStream("InputStream", input.outputPort, dimensions.inputEvent).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("DimensionalData", dimensions.output, store.input);
    dag.addStream("Query", queryPort, store.query);
    dag.addStream("QueryResult", store.queryResult, queryResultPort);
  }
}

