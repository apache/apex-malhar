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
package com.datatorrent.demos.adsdimension;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.hds.tfile.TFileImpl;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import com.datatorrent.contrib.kafka.SimpleKafkaConsumer;
import com.datatorrent.lib.statistics.DimensionsComputation;

/**
 * An AdsDimensionsDemo run with HDS
 *
 * Example of configuration
 <pre>
 {@code
 <property>
 <name>dt.application.AdsDimensionsGenericDemo.class</name>
 <value>com.datatorrent.demos.adsdimension.GenericApplication</value>
 </property>

 <property>
 <name>dt.application.AdsDimensionsGenericDemo.attr.containerMemoryMB</name>
 <value>8192</value>
 </property>

 <property>
 <name>dt.application.AdsDimensionsGenericDemo.attr.containerJvmOpts</name>
 <value>-Xmx6g -server -Dlog4j.debug=true -Xloggc:&lt;LOG_DIR&gt;/gc.log -verbose:gc -XX:+PrintGCDateStamps</value>
 </property>

 <property>
 <name>dt.application.AdsDimensionsGenericDemo.port.*.attr.QUEUE_CAPACITY</name>
 <value>32000</value>
 </property>

 <property>
 <name>dt.operator.InputGenerator.attr.INITIAL_PARTITION_COUNT</name>
 <value>8</value>
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
 <name>dt.operator.HDSOut.attr.INITIAL_PARTITION_COUNT</name>
 <value>4</value>
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
 */
@ApplicationAnnotation(name="AdsDimensionsGenericDemo")
public class GenericApplication implements StreamingApplication
{

  public static EventSchema getDataDesc() {
    EventSchema eDesc = new EventSchema();

    Map<String, Class> dataDesc  = Maps.newHashMap();
    dataDesc.put("timestamp", Long.class);
    dataDesc.put("pubId", Integer.class);
    dataDesc.put("adId", Integer.class);
    dataDesc.put("adUnit", Integer.class);

    dataDesc.put("clicks", Long.class);
    eDesc.setDataDesc(dataDesc);

    String[] keys = { "timestamp", "pubId", "adId", "adUnit" };
    List<String> keyDesc = Lists.newArrayList(keys);
    eDesc.setKeys(keyDesc);

    Map<String, String> aggrDesc = Maps.newHashMap();
    aggrDesc.put("clicks", "sum");
    eDesc.setAggrDesc(aggrDesc);

    return eDesc;
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    EventSchema dataDesc = getDataDesc();
    dag.setAttribute(DAG.APPLICATION_NAME, "AdsDimensionsGeneric");
    InputItemGenerator input = dag.addOperator("InputGenerator", InputItemGenerator.class);

    AdEventToMapConverter converter = dag.addOperator("Converter", new AdEventToMapConverter());

    DimensionsComputation<Map<String, Object>, MapAggregate> dimensions = dag.addOperator("DimensionsComputation", new DimensionsComputation<Map<String, Object>, MapAggregate>());
    dag.getMeta(dimensions).getAttributes().put(Context.OperatorContext.APPLICATION_WINDOW_COUNT, 4);
    String[] dimensionSpecs = new String[] {
        "time=" + TimeUnit.MINUTES,
        "time=" + TimeUnit.MINUTES + ":adUnit",
        "time=" + TimeUnit.MINUTES + ":adId",
        "time=" + TimeUnit.MINUTES + ":pubId",
        "time=" + TimeUnit.MINUTES + ":adId:adUnit",
        "time=" + TimeUnit.MINUTES + ":pubId:adUnit",
        "time=" + TimeUnit.MINUTES + ":pubId:adId",
        "time=" + TimeUnit.MINUTES + ":pubId:adId:adUnit"
    };

    MapAggregator[] aggregators = new MapAggregator[dimensionSpecs.length];
    for (int i = dimensionSpecs.length; i-- > 0;) {
      MapAggregator aggregator = new MapAggregator(dataDesc);
      aggregator.init(dimensionSpecs[i]);
      aggregators[i] = aggregator;
    }
    dimensions.setAggregators(aggregators);

    MapDimensionStoreOperator hdsOut = dag.addOperator("HDSOut", MapDimensionStoreOperator.class);
    TFileImpl hdsFile = new TFileImpl.DefaultTFileImpl();
    hdsOut.setFileStore(hdsFile);
    hdsOut.setEventDesc(dataDesc);
    hdsOut.setAggregator(new MapAggregator(dataDesc));

    KafkaSinglePortStringInputOperator queries = dag.addOperator("Query", new KafkaSinglePortStringInputOperator());
    queries.setConsumer(new SimpleKafkaConsumer());

    KafkaSinglePortOutputOperator<Object, Object> queryResult = dag.addOperator("QueryResult", new KafkaSinglePortOutputOperator<Object, Object>());
    queryResult.getConfigProperties().put("serializer.class", com.datatorrent.demos.adsdimension.KafkaJsonEncoder.class.getName());

    dag.addStream("InputStream", input.outputPort, converter.in).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("ConvertingStream", converter.out, dimensions.data).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("DimensionalData", dimensions.output, hdsOut.input);
    dag.addStream("Query", queries.outputPort, hdsOut.query);
    dag.addStream("QueryResult", hdsOut.queryResult, queryResult.inputPort);
  }

}
