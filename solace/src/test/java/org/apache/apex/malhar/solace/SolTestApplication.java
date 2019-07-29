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
package org.apache.apex.malhar.solace;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;

import com.solacesystems.jcsmp.JCSMPProperties;
import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.stream.Counter;

//import com.datatorrent.api.Context.PortContext;
//import com.datatorrent.lib.util.BaseKeyValueOperator.DefaultPartitionCodec;
//import org.apache.hadoop.fs.Path;

/**
 *
 */
@ApplicationAnnotation(name = "SolTestApp")
public class SolTestApplication implements StreamingApplication
{
  private static final Logger logger = LoggerFactory.getLogger(SolTestApplication.class);
  private static final String DT_SOLACE_PROP_IDENTIFIER = "dt.solace";
  private static final String DT_SOLACE_PROP_SEARCH = DT_SOLACE_PROP_IDENTIFIER + "\\..*";
  private static final int DT_SOLACE_PROP_INDEX = DT_SOLACE_PROP_IDENTIFIER.length() + 1;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    Map<String, String> props = conf.getValByRegex(DT_SOLACE_PROP_SEARCH);
    logger.info("{}", props);
    JCSMPProperties properties = new JCSMPProperties();

    for (Map.Entry<String, String> entry : props.entrySet()) {

      properties.setProperty(entry.getKey().substring(DT_SOLACE_PROP_INDEX), entry.getValue());
      logger.info("Property: {} Value: {}", entry.getKey().substring(DT_SOLACE_PROP_INDEX), entry.getValue());
    }

    //Used for sticky partitioning
    //@SuppressWarnings("unused")
    //DefaultPartitionCodec<String, Double> codec = new DefaultPartitionCodec<String, Double>();

    dag.setAttribute(Context.OperatorContext.CHECKPOINT_WINDOW_COUNT, 20);
    dag.setAttribute(Context.DAGContext.CHECKPOINT_WINDOW_COUNT, 20);

    /*====ECS processing====*/
    //Test for Guaranteed Solace consumers
    //SolaceGuaranteedTextStrInputOperator ecsInput = dag.addOperator("SolaceEcsInput", SolaceGuaranteedTextStrInputOperator.class);
    SolaceReliableTextStrInputOperator ecsInput = dag.addOperator("SolaceEcsInput", SolaceReliableTextStrInputOperator.class);
    //Test for Reliable Solace consumers
    ecsInput.setProperties(properties);
    dag.setAttribute(ecsInput, Context.OperatorContext.TIMEOUT_WINDOW_COUNT, 600);


    Counter counter = dag.addOperator("Counter", new Counter());


    ConsoleOutputOperator console = dag.addOperator("Console", new ConsoleOutputOperator());


    dag.addStream("parseEcsXml", ecsInput.output, counter.input);
    dag.addStream("console", counter.output, console.input);

  }

}
