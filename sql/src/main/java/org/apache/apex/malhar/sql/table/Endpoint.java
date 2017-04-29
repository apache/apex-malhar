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

import org.apache.apex.malhar.sql.planner.RelInfo;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.DAG;

/**
 * This interface defines abstract table and how it should be operated with.
 * Endpoint interface can be implemented for any type of data source eg. Kafka, File, JDBC etc.
 * Implementation of Endpoint interface should define how the table should represented for both input OR output side.
 *
 * @since 3.6.0
 */
@InterfaceStability.Evolving
public interface Endpoint
{
  String ENDPOINT = "endpoint";
  String SYSTEM_OPERANDS = "endpointOperands";

  /**
   * Returns target type system
   * @return Returns target type system
   */
  EndpointType getTargetType();

  /**
   * Set Endpoint operands. This method is used when the table definitions are provided using calcite schema format.
   * This is the map which is present against key "endpointOperands" in calcite schema definition input file.
   *
   * @param operands Map of endpoint operands.
   */
  void setEndpointOperands(Map<String, Object> operands);

  /**
   * Message Format type which defines how the data should be interpreted for both input and output side.
   *
   * @param messageFormat Object of type MessageFormat
   */
  void setMessageFormat(MessageFormat messageFormat);

  /**
   * Implementation of this method should populate Apex DAG if this table is at input side of pipeline.
   *
   * @param dag {@link DAG} object to be populated
   * @param typeFactory Java Type Factory
   *
   * @return Returns {@link RelInfo} describing output of this input phase.
   */
  RelInfo populateInputDAG(DAG dag, JavaTypeFactory typeFactory);

  /**
   * Implementation of this method should populate Apex DAG if table is at output side of pipeline.
   *
   * @param dag {@link DAG} object to be populated
   * @param typeFactory Java Type Factory
   * @return Returns {@link RelInfo} describing expected input of this output phase.
   */
  RelInfo populateOutputDAG(DAG dag, JavaTypeFactory typeFactory);

  /**
   * This method returns what should be the input data type to output phase OR output data type of input phase.
   *
   * @param typeFactory Java Type Factory for data type conversions.
   *
   * @return {@link RelDataType} representing data type format.
   */
  RelDataType getRowType(RelDataTypeFactory typeFactory);

  /**
   * Type of Endpoints
   */
  enum EndpointType
  {
    FILE,
    KAFKA,
    PORT
  }
}
