/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.sql.sample;

import java.util.Date;
import java.util.Map;

import org.apache.apex.malhar.contrib.parser.CsvParser;
import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.apex.malhar.sql.SQLExecEnvironment;
import org.apache.apex.malhar.sql.table.CSVMessageFormat;
import org.apache.apex.malhar.sql.table.FileEndpoint;
import org.apache.apex.malhar.sql.table.StreamEndpoint;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.ImmutableMap;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.util.BaseOperator;


@ApplicationAnnotation(name = "FusionStyleSQLApplication")
/**
 * @since 3.6.0
 */
public class FusionStyleSQLApplication implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    SQLExecEnvironment env = SQLExecEnvironment.getEnvironment();
    env.registerFunction("APEXCONCAT", PureStyleSQLApplication.class, "apex_concat_str");

    Map<String, Class> fieldMapping = ImmutableMap.<String, Class>of(
        "RowTime", Date.class,
        "id", Integer.class,
        "Product", String.class,
        "units", Integer.class);

    // Add Kafka Input
    KafkaSinglePortInputOperator kafkaInput = dag.addOperator("KafkaInput", KafkaSinglePortInputOperator.class);
    kafkaInput.setInitialOffset("EARLIEST");

    // Add CSVParser
    CsvParser csvParser = dag.addOperator("CSVParser", CsvParser.class);
    dag.addStream("KafkaToCSV", kafkaInput.outputPort, csvParser.in);

    // Register CSV Parser output as input table for first SQL
    env.registerTable(conf.get("sqlSchemaInputName"), new StreamEndpoint(csvParser.out, fieldMapping));

    // Register FileEndpoint as output table for second SQL.
    env.registerTable(conf.get("sqlSchemaOutputName"), new FileEndpoint(conf.get("folderPath"),
        conf.get("fileName"), new CSVMessageFormat(conf.get("sqlSchemaOutputDef"))));

    // Add second SQL to DAG
    env.executeSQL(dag, conf.get("sql"));
  }

  public static class PassThroughOperator extends BaseOperator
  {
    public final transient DefaultOutputPort output = new DefaultOutputPort();
    public final transient DefaultInputPort input = new DefaultInputPort()
    {
      @Override
      public void process(Object o)
      {
        output.emit(output);
      }
    };
  }
}
