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
package org.apache.apex.malhar.sql.table;

import java.util.Map;

import org.apache.apex.malhar.lib.fs.GenericFileOutputOperator;
import org.apache.apex.malhar.sql.operators.LineReader;
import org.apache.apex.malhar.sql.operators.OperatorUtils;
import org.apache.apex.malhar.sql.planner.RelInfo;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;

/**
 * This is an implementation of {@link Endpoint} which defined how data should be read/written to file system.
 *
 * @since 3.6.0
 */
@InterfaceStability.Evolving
public class FileEndpoint implements Endpoint
{
  public static final String FILE_INPUT_DIRECTORY = "directory";
  public static final String FILE_OUT_PATH = "outputFilePath";
  public static final String FILE_OUT_NAME = "outputFileName";

  private MessageFormat messageFormat;

  private Map<String, Object> operands;

  public FileEndpoint()
  {
  }

  public FileEndpoint(String directory, MessageFormat messageFormat)
  {
    this.messageFormat = messageFormat;
    this.operands = ImmutableMap.<String, Object>of(FILE_INPUT_DIRECTORY, directory);
  }

  public FileEndpoint(String directory, String fileName, MessageFormat messageFormat)
  {
    this.messageFormat = messageFormat;
    this.operands = ImmutableMap.<String, Object>of(FILE_OUT_PATH, directory, FILE_OUT_NAME, fileName);
  }

  @Override
  public EndpointType getTargetType()
  {
    return EndpointType.FILE;
  }

  @Override
  public void setEndpointOperands(Map<String, Object> operands)
  {
    this.operands = operands;
  }

  @Override
  public void setMessageFormat(MessageFormat messageFormat)
  {
    this.messageFormat = messageFormat;
  }

  @Override
  public RelInfo populateInputDAG(DAG dag, JavaTypeFactory typeFactory)
  {
    LineReader fileInput = dag.addOperator(OperatorUtils.getUniqueOperatorName("FileInput"), LineReader.class);
    fileInput.setDirectory((String)operands.get(FILE_INPUT_DIRECTORY));

    RelInfo spec = messageFormat.populateInputDAG(dag, typeFactory);
    dag.addStream(OperatorUtils.getUniqueStreamName("File", "Parser"), fileInput.output, spec.getInputPorts().get(0));
    return new RelInfo("Input", Lists.<Operator.InputPort>newArrayList(), spec.getOperator(), spec.getOutPort(),
      messageFormat.getRowType(typeFactory));
  }

  @Override
  public RelInfo populateOutputDAG(DAG dag, JavaTypeFactory typeFactory)
  {
    RelInfo spec = messageFormat.populateOutputDAG(dag, typeFactory);

    GenericFileOutputOperator.StringFileOutputOperator fileOutput =
        dag.addOperator(OperatorUtils.getUniqueOperatorName("FileOutput"),
        GenericFileOutputOperator.StringFileOutputOperator.class);
    fileOutput.setFilePath((String)operands.get(FILE_OUT_PATH));
    fileOutput.setOutputFileName((String)operands.get(FILE_OUT_NAME));

    dag.addStream(OperatorUtils.getUniqueStreamName("Formatter", "File"), spec.getOutPort(), fileOutput.input);

    return new RelInfo("Output", spec.getInputPorts(), spec.getOperator(), null, messageFormat.getRowType(typeFactory));
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory)
  {
    return messageFormat.getRowType(typeFactory);
  }
}
