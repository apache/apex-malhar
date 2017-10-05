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

package org.apache.apex.examples.transform;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.transform.TransformOperator;

/**
 * @since 3.7.0
 */
@ApplicationAnnotation(name = "TransformExample")
public class Application implements StreamingApplication
{
  public static class Collector extends BaseOperator
  {
    private static long maxLength = 10;
    private static ArrayList<Object> results = new ArrayList<Object>();

    @OutputPortFieldAnnotation(schemaRequired = true)
    public transient DefaultOutputPort<Object> output = new DefaultOutputPort<>();

    @InputPortFieldAnnotation(schemaRequired = true)
    public transient DefaultInputPort<Object> input = new DefaultInputPort<Object>()
    {
      @Override
      public void process(Object tuple)
      {
        output.emit(tuple);
        while (results.size() >= maxLength) {
          results.remove(0);
        }
        results.add(tuple);
      }
    };


    public static ArrayList<Object> getResult()
    {
      return results;
    }

    public static long getMaxLength()
    {
      return maxLength;
    }

    public void setMaxLength(long maxLength)
    {
      this.maxLength = maxLength;
    }

  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    POJOGenerator input = dag.addOperator("Input", new POJOGenerator());
    TransformOperator transform = dag.addOperator("Process", new TransformOperator());
    // Set expression map
    Map<String, String> expMap = new HashMap<>();
    expMap.put("name", "{$.firstName}.concat(\" \").concat({$.lastName})");
    expMap.put("age", "(new java.util.Date()).getYear() - {$.dateOfBirth}.getYear()");
    expMap.put("address", "{$.address}.toLowerCase()");
    transform.setExpressionMap(expMap);
    ConsoleOutputOperator output = dag.addOperator("Output", new ConsoleOutputOperator());
    output.setSilent(true);
    Collector collector = dag.addOperator("collector", new Collector());

    dag.addStream("InputToTransform", input.output, transform.input);
    dag.addStream("TransformToCollector", transform.output, collector.input);
    dag.addStream("TransformToOutput", collector.output, output.input);

    dag.setInputPortAttribute(transform.input, Context.PortContext.TUPLE_CLASS, CustomerEvent.class);
    dag.setOutputPortAttribute(transform.output, Context.PortContext.TUPLE_CLASS, CustomerInfo.class);
    dag.setInputPortAttribute(collector.input, Context.PortContext.TUPLE_CLASS, CustomerInfo.class);
    dag.setOutputPortAttribute(collector.output, Context.PortContext.TUPLE_CLASS, CustomerInfo.class);
    dag.setAttribute(transform, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<TransformOperator>(2));
  }
}
