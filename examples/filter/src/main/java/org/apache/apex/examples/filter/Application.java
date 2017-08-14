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


package org.apache.apex.examples.filter;

import org.apache.apex.malhar.contrib.formatter.CsvFormatter;
import org.apache.apex.malhar.contrib.parser.CsvParser;
import org.apache.apex.malhar.lib.filter.FilterOperator;
import org.apache.apex.malhar.lib.fs.FSRecordReaderModule;
import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator.StringFileOutputOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * Simple application illustrating filter operator
 *
 * @since 3.7.0
 */
@ApplicationAnnotation(name = "FilterExample")
public class Application implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    FSRecordReaderModule recordReader = dag.addModule("recordReader", FSRecordReaderModule.class);
    CsvParser csvParser = dag.addOperator("csvParser", CsvParser.class);
    FilterOperator filterOperator = dag.addOperator("filterOperator", new FilterOperator());

    CsvFormatter selectedFormatter = dag.addOperator("selectedFormatter", new CsvFormatter());
    CsvFormatter rejectedFormatter = dag.addOperator("rejectedFormatter", new CsvFormatter());

    StringFileOutputOperator selectedOutput = dag.addOperator("selectedOutput", new StringFileOutputOperator());
    StringFileOutputOperator rejectedOutput = dag.addOperator("rejectedOutput", new StringFileOutputOperator());

    dag.addStream("record", recordReader.records, csvParser.in);
    dag.addStream("pojo", csvParser.out, filterOperator.input);

    dag.addStream("pojoSelected", filterOperator.truePort, selectedFormatter.in);
    dag.addStream("pojoRejected", filterOperator.falsePort, rejectedFormatter.in);

    dag.addStream("csvSelected", selectedFormatter.out, selectedOutput.input);
    dag.addStream("csvRejected", rejectedFormatter.out, rejectedOutput.input);
  }
}
