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

import org.apache.apex.malhar.sql.SQLExecEnvironment;
import org.apache.apex.malhar.sql.table.CSVMessageFormat;
import org.apache.apex.malhar.sql.table.FileEndpoint;
import org.apache.apex.malhar.sql.table.KafkaEndpoint;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "PureStyleSQLApplication")
/**
 * @since 3.6.0
 */
public class PureStyleSQLApplication implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Source definition
    String schemaInName = conf.get("schemaInName");
    String schemaInDef = conf.get("schemaInDef");
    String broker = conf.get("broker");
    String sourceTopic = conf.get("topic");

    // Destination definition
    String schemaOutName = conf.get("schemaOutName");
    String schemaOutDef = conf.get("schemaOutDef");
    String outputFolder = conf.get("outputFolder");
    String outFilename = conf.get("destFileName");

    // SQL statement
    String sql = conf.get("sql");

    SQLExecEnvironment.getEnvironment()
        .registerTable(schemaInName, new KafkaEndpoint(broker, sourceTopic,
            new CSVMessageFormat(schemaInDef)))
        .registerTable(schemaOutName, new FileEndpoint(outputFolder, outFilename,
            new CSVMessageFormat(schemaOutDef)))
        .registerFunction("APEXCONCAT", this.getClass(), "apex_concat_str")
        .executeSQL(dag, sql);
  }

  public static String apex_concat_str(String s1, String s2)
  {
    return s1 + s2;
  }
}
