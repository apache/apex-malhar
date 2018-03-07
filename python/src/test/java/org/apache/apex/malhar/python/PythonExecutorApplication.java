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

package org.apache.apex.malhar.python;

import org.apache.apex.malhar.lib.function.Function;
import org.apache.apex.malhar.lib.function.FunctionOperator;
import org.apache.apex.malhar.lib.function.FunctionOperatorUtil;
import org.apache.hadoop.conf.Configuration;

import com.google.common.annotations.VisibleForTesting;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "PythonOperatorExample")
/**
 * @since 3.8.0
 */
public class PythonExecutorApplication implements StreamingApplication
{
  @VisibleForTesting
  Function.MapFunction<Object, ?> outputFn = FunctionOperatorUtil.CONSOLE_SINK_FN;

  @VisibleForTesting
  PythonPayloadPOJOGenerator pojoDataGenerator;

  @VisibleForTesting
  SimplePythonOpOperator simplePythonOpOperator;

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    pojoDataGenerator = dag.addOperator("Input", new PythonPayloadPOJOGenerator());
    simplePythonOpOperator = dag.addOperator("pythonprocessor", new SimplePythonOpOperator());
    FunctionOperator.MapFunctionOperator<Object, ?> output = dag.addOperator("out",
        new FunctionOperator.MapFunctionOperator<>(outputFn));
    dag.addStream("InputToPython", pojoDataGenerator.output, simplePythonOpOperator.input);
    dag.addStream("PythonToOutput", simplePythonOpOperator.outputPort, output.input);

  }

}
