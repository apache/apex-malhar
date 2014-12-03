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
package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.api.Context;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;

/**
 * DimensionsDemo run with HDS
 *
 * Following settings are provided with properties.xml set by default, but can be modified in local dt-site.xml
 <pre>
 {@code

 <property>
 <name>dt.application.GenericDimensionsApplication.attr.CONTAINER_MEMORY_MB</name>
 <value>8192</value>
 </property>
 <property>
 <name>dt.application.GenericDimensionsApplication.attr.containerJvmOpts</name>
 <value>-Xmx6g -server -Dlog4j.debug=true -Xloggc:&lt;LOG_DIR&gt;/gc.log -verbose:gc -XX:+PrintGCDateStamps</value>
 </property>
 <property>
 <name>dt.application.GenericDimensionsApplication.port.*.attr.QUEUE_CAPACITY</name>
 <value>32000</value>
 </property>
 <property>
 <name>dt.application.GenericDimensionsApplication.operator.InputGenerator.attr.PARTITIONER</name>
 <value>com.datatorrent.lib.partitioner.StatelessPartitioner:2</value>
 </property>
 <property>
 <name>dt.application.GenericDimensionsApplication.operator.InputGenerator.maxTuplesPerWindow</name>
 <value>40000</value>
 </property>
 <property>
 <name>dt.application.GenericDimensionsApplication.operator.DimensionsComputation.attr.APPLICATION_WINDOW_COUNT</name>
 <value>4</value>
 </property>
 <property>
 <name>dt.application.GenericDimensionsApplication.operator.DimensionsStore.attr.PARTITIONER</name>
 <value>com.datatorrent.lib.partitioner.StatelessPartitioner:4</value>
 </property>
 <property>
 <name>dt.application.GenericDimensionsApplication.operator.DimensionsStore.fileStore.basePath</name>
 <value>GenericDimensionsApplication</value>
 </property>
 <property>
 <name>dt.application.GenericDimensionsApplication.operator.DimensionsStore.prop.maxCacheSize</name>
 <value>5</value>
 </property>
 <property>
 <name>dt.application.GenericDimensionsApplication.operator.Query.topic</name>
 <value>GenericDimensionsQuery</value>
 </property>
 <property>
 <name>dt.application.GenericDimensionsApplication.operator.QueryResult.topic</name>
 <value>GenericDimensionsQueryResult</value>
 </property>
 }
 </pre>
 *
 *
 *
 * Following settings should be provided by user and modified to reflect local Kafka settings
 *
 *
 <pre>
 {@code

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
@ApplicationAnnotation(name="GenericDimensionsApplication")
public class GenericDimensionsApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    JsonSalesGenerator input = dag.addOperator("Input", JsonSalesGenerator.class);
    input.setAddProductCategory(true);
    JsonToMapConverter converter = dag.addOperator("Parse", JsonToMapConverter.class);
    GenericDimensionComputation dimensions = dag.addOperator("Compute", new GenericDimensionComputation());
    DimensionStoreOperator store = dag.addOperator("Store", DimensionStoreOperator.class);
    KafkaSinglePortStringInputOperator queries = dag.addOperator("Query", new KafkaSinglePortStringInputOperator());
    KafkaSinglePortOutputOperator<Object, Object> queryResult = dag.addOperator("QueryResult", new KafkaSinglePortOutputOperator<Object, Object>());

    dag.setInputPortAttribute(converter.input, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(dimensions.data, Context.PortContext.PARTITION_PARALLEL, true);

    // Removing setLocality(Locality.CONTAINER_LOCAL) from JSONStream and MapStream to isolate performance bottleneck
    dag.addStream("JSONStream", input.jsonBytes, converter.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("MapStream", converter.outputMap, dimensions.data).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("DimensionalData", dimensions.output, store.input);
    dag.addStream("Query", queries.outputPort, store.query);
    dag.addStream("QueryResult", store.queryResult, queryResult.inputPort);
  }

}
