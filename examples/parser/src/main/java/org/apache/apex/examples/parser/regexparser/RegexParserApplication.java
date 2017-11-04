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
package org.apache.apex.examples.parser.regexparser;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.parser.RegexParser;

@ApplicationAnnotation(name = "RegexParser")
/**
 * @since 3.8.0
 */
public class RegexParserApplication implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    ServerLogGenerator logGenerator = dag.addOperator("logGenerator", ServerLogGenerator.class);
    RegexParser regexParser = dag.addOperator("regexParser", RegexParser.class);
    dag.setOutputPortAttribute(regexParser.out, Context.PortContext.TUPLE_CLASS, ServerLog.class);

    FileOutputOperator regexWriter = dag.addOperator("regexWriter", FileOutputOperator.class);
    FileOutputOperator regexErrorWriter = dag.addOperator("regexErrorWriter", FileOutputOperator.class);

    dag.addStream("regexInput", logGenerator.outputPort, regexParser.in);
    dag.addStream("regexOutput", regexParser.out, regexWriter.input);
    dag.addStream("regexError", regexParser.err, regexErrorWriter.input);
  }
}
