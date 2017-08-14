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

package org.apache.apex.examples.csvformatter;

import org.apache.apex.malhar.contrib.formatter.CsvFormatter;
import org.apache.apex.malhar.contrib.parser.JsonParser;
import org.apache.apex.malhar.lib.appdata.schemas.SchemaUtils;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "CustomOutputFormatter")
/**
 * @since 3.7.0
 */
public class Application implements StreamingApplication
{
  //Set the delimiters and schema structure  for the custom output in schema.json
  private static final String filename = "schema.json";

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    JsonGenerator generator = dag.addOperator("JsonGenerator", JsonGenerator.class);
    JsonParser jsonParser = dag.addOperator("jsonParser", JsonParser.class);

    CsvFormatter formatter = dag.addOperator("formatter", CsvFormatter.class);
    formatter.setSchema(SchemaUtils.jarResourceFileToString(filename));
    dag.setInputPortAttribute(formatter.in, PortContext.TUPLE_CLASS, PojoEvent.class);

    HDFSOutputOperator<String> hdfsOutput = dag.addOperator("HDFSOutputOperator", HDFSOutputOperator.class);
    hdfsOutput.setLineDelimiter("");

    dag.addStream("parserStream", generator.out, jsonParser.in);
    dag.addStream("formatterStream", jsonParser.out, formatter.in);
    dag.addStream("outputStream", formatter.out, hdfsOutput.input);

  }
}
