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

import java.util.HashMap;
import java.util.Map;

import org.apache.apex.malhar.lib.function.Function;
import org.apache.apex.malhar.lib.function.FunctionOperator;
import org.apache.apex.malhar.lib.function.FunctionOperatorUtil;
import org.apache.apex.malhar.lib.transform.TransformOperator;
import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;

@ApplicationAnnotation(name = "TransformExample")
/**
 * @since 3.7.0
 */
public class SimpleTransformApplication implements StreamingApplication
{
  @VisibleForTesting
  Function.MapFunction<Object, ?> outputFn = FunctionOperatorUtil.CONSOLE_SINK_FN;

  @VisibleForTesting
  POJOGenerator pojoDataGenerator;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    pojoDataGenerator = dag.addOperator("Input", new POJOGenerator());
    TransformOperator transform = dag.addOperator("Process", new TransformOperator());
    // Set expression map
    Map<String, String> expMap = new HashMap<>();
    expMap.put("name", "{$.firstName}.concat(\" \").concat({$.lastName})");
    expMap.put("age", "(new java.util.Date()).getYear() - {$.dateOfBirth}.getYear()");
    expMap.put("address", "{$.address}.toLowerCase()");
    transform.setExpressionMap(expMap);
    FunctionOperator.MapFunctionOperator<Object, ?> output = dag.addOperator("out",
        new FunctionOperator.MapFunctionOperator<>(outputFn));

    dag.addStream("InputToTransform", pojoDataGenerator.output, transform.input);
    dag.addStream("TransformToOutput", transform.output, output.input);

    dag.setInputPortAttribute(transform.input, Context.PortContext.TUPLE_CLASS, CustomerEvent.class);
    dag.setOutputPortAttribute(transform.output, Context.PortContext.TUPLE_CLASS, CustomerInfo.class);
    setPartitioner(dag,conf,transform);
  }

  void setPartitioner(DAG dag,Configuration conf, TransformOperator transform)
  {
    dag.setAttribute(transform, Context.OperatorContext.PARTITIONER,
        new StatelessPartitioner<TransformOperator>(2));
  }
}
