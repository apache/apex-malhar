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
package org.apache.apex.malhar.sql.operators;

import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.classification.InterfaceStability;

@InterfaceStability.Evolving
/**
 * @since 3.6.0
 */
public class OperatorUtils
{
  private static int opCount = 1;
  private static int streamCount = 1;

  /**
   * This method generates unique name for the operator.
   *
   * @param operatorType Base name of the operator.
   * @return Returns unique name of the operator.
   */
  public static String getUniqueOperatorName(String operatorType)
  {
    return operatorType + "_" + opCount++;
  }

  /**
   * This method generates unique name for the stream using input and output.
   *
   * @param output Name of the output end of the stream
   * @param input Name of the input end of the stream
   * @return Returns unique name of the stream.
   */
  public static String getUniqueStreamName(String output, String input)
  {
    return output + "_" + input + "_" + streamCount++;
  }

  /**
   * This method gives field name for POJO class for given {@link RelDataTypeField} object.
   *
   * @param field field object that needs to be converted to POJO class field name
   * @return Return field name from POJO class
   */
  public static String getFieldName(RelDataTypeField field)
  {
    SqlTypeName sqlTypeName = field.getType().getSqlTypeName();
    String name = getValidFieldName(field);

    name = (sqlTypeName == SqlTypeName.TIMESTAMP) ?
      (name + "Ms") :
      ((sqlTypeName == SqlTypeName.DATE) ? (name + "Sec") : name);

    return name;
  }

  public static String getValidFieldName(RelDataTypeField field)
  {
    String name = field.getName().replaceAll("\\W", "");
    name = (name.length() == 0) ? "f0" : name;
    return name;
  }

}
