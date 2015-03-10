/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.contrib.kafka.KafkaSinglePortOutputOperator;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * DimensionsDemo run with CsvToMapParser
 *
 * Following settings are provided with properties.xml set by default, but can be modified in local dt-site.xml
 <pre>
 {@code

 <property>
 <name>dt.application.GenericDimensionsApplication.operator.*.attr.MEMORY_MB</name>
 <value>8192</value>
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
 * @since 2.0.0
 */
@ApplicationAnnotation(name="GenericDimensionsWithCsvMapParser")
public class GenericDimensionsWithCsvMapParser implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    CsvToMapParser parser = dag.addOperator("Parser", CsvToMapParser.class);
    String filepath = conf.get("dt.application.CsvParserFileMappingInputApplication.operator.CsvParser.fieldmappingFile");
    parser.setFieldmappingFile(filepath);
    createFieldMappingFile(filepath);
    parser.setIsHeader(false);
    parser.setFieldDelimiter(',');
    parser.setLineDelimiter("\n");
    GenericDimensionComputation dimensions = dag.addOperator("Compute", new GenericDimensionComputation());
    DimensionStoreOperator store = dag.addOperator("Store", DimensionStoreOperator.class);
    KafkaSinglePortStringInputOperator queries = dag.addOperator("Query", new KafkaSinglePortStringInputOperator());
    KafkaSinglePortOutputOperator<Object, Object> queryResult = dag.addOperator("QueryResult", new KafkaSinglePortOutputOperator<Object, Object>());

    dag.setInputPortAttribute(parser.input, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(dimensions.data, Context.PortContext.PARTITION_PARALLEL, true);

    dag.addStream("MapStream", parser.output, dimensions.data).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("DimensionalData", dimensions.output, store.input);
    dag.addStream("Query", queries.outputPort, store.query);
    dag.addStream("QueryResult", store.queryResult, queryResult.inputPort);
  }

  public void createFieldMappingFile(String filepath)
  {
    FileSystem hdfs = null;
    //Creating a file in HDFS
    Path newFilePath = new Path(filepath);
    try {
      hdfs = FileSystem.get(new Configuration());
      hdfs.createNewFile(newFilePath);
    }
    catch (IOException ex) {
      DTThrowable.rethrow(ex);
    }
    //Writing data to a HDFS file
    StringBuilder sb = new StringBuilder();
    sb.append("publisherId");
    sb.append(":");
    sb.append("INTEGER");
    sb.append("\n");
    sb.append("advertiserId");
    sb.append(":");
    sb.append("INTEGER");
    sb.append("\n");
    sb.append("adUnit");
    sb.append(":");
    sb.append("INTEGER");
    sb.append("\n");
    sb.append("timestamp");
    sb.append(":");
    sb.append("LONG");
    sb.append("\n");
    sb.append("cost");
    sb.append(":");
    sb.append("DOUBLE");
    sb.append("\n");
    sb.append("revenue");
    sb.append(":");
    sb.append("DOUBLE");
    sb.append("\n");
    sb.append("impressions");
    sb.append(":");
    sb.append("LONG");
    sb.append("\n");
    sb.append("clicks");
    sb.append(":");
    sb.append("LONG");
    sb.append("\n");

    byte[] byt = sb.toString().getBytes();
    try {
      FSDataOutputStream fsOutStream = hdfs.create(newFilePath);
      fsOutStream.write(byt);
      fsOutStream.close();
    }
    catch (IOException ex) {
      DTThrowable.rethrow(ex);

    }
  }
}
