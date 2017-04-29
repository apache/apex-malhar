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
 * This interface defines how message should be parsed from input or formatted for output.
 * The implementation of this interface should define both parsing and formatting representation for data.
 *
 * @since 3.6.0
 */
@InterfaceStability.Evolving
public interface MessageFormat
{
  String MESSAGE_FORMAT = "messageFormat";
  String MESSAGE_FORMAT_OPERANDS = "messageFormatOperands";

  /**
   * Gives type of {@link MessageFormat}
   * @return Returns type of {@link MessageFormat}
   */
  MessageFormatType getMessageFormatType();

  /**
   * Set messageFormat operands. This method is used when the table definitions are provided using calcite schema format.
   * This is the map which is present against key "endpointOperands" in calcite schema definition input file.
   * @param operands
   */
  void setMessageFormatOperands(Map<String, Object> operands);

  /**
   * Implementation of this method should populate the DAG for parsing logic for the data received from {@link Endpoint}
   *
   * @param dag {@link DAG} object to be populated
   * @param typeFactory Java Type Factory
   * @return Returns {@link RelInfo} defining output data type definition after parsing of data.
   */
  RelInfo populateInputDAG(DAG dag, JavaTypeFactory typeFactory);

  /**
   * Implementation of this method should populate the DAG for formatting logic of data to be written to {@link Endpoint}
   *
   * @param dag {@link DAG} object to be populated
   * @param typeFactory Java Type Factory
   * @return Returns {@link RelInfo} defining expected input for formatting ot data.
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
   * Message Format types
   */
  enum MessageFormatType
  {
    CSV
  }
}
