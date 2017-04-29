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

import java.lang.reflect.Field;
import java.util.Date;
import java.util.Map;

import org.apache.apex.malhar.sql.planner.RelInfo;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.collect.Lists;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;

/**
 * This is an implementation of {@link Endpoint} which defined how data should be read/written to a Apex streaming port.
 *
 * @since 3.6.0
 */
@InterfaceStability.Evolving
public class StreamEndpoint implements Endpoint
{
  private Operator.InputPort inputPort;
  private Operator.OutputPort outputPort;
  private Class pojoClass;
  private Map<String, Class> fieldMapping;

  public StreamEndpoint(Operator.InputPort port, Class pojoClass)
  {
    this.inputPort = port;
    this.pojoClass = pojoClass;
  }

  public StreamEndpoint(Operator.InputPort port, Map<String, Class> fieldMapping)
  {
    this.inputPort = port;
    this.fieldMapping = fieldMapping;
  }

  public StreamEndpoint(Operator.OutputPort outputPort, Class pojoClass)
  {
    this.outputPort = outputPort;
    this.pojoClass = pojoClass;
  }

  public StreamEndpoint(Operator.OutputPort port, Map<String, Class> fieldMapping)
  {
    this.outputPort = port;
    this.fieldMapping = fieldMapping;
  }

  @Override
  public EndpointType getTargetType()
  {
    return EndpointType.PORT;
  }

  @Override
  public void setEndpointOperands(Map<String, Object> operands)
  {
  }

  @Override
  public void setMessageFormat(MessageFormat messageFormat)
  {
  }

  @Override
  public RelInfo populateInputDAG(DAG dag, JavaTypeFactory typeFactory)
  {
    return new RelInfo("StreamInput", Lists.<Operator.InputPort>newArrayList(), null, outputPort, getRowType(typeFactory));
  }

  @Override
  public RelInfo populateOutputDAG(DAG dag, JavaTypeFactory typeFactory)
  {
    return new RelInfo("StreamOutput", Lists.newArrayList(inputPort), null, null, getRowType(typeFactory));
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory)
  {
    RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();
    if (fieldMapping != null) {
      for (Map.Entry<String, Class> entry : fieldMapping.entrySet()) {
        builder.add(entry.getKey(), convertField(typeFactory, entry.getValue()));
      }
    } else if (pojoClass != null) {
      for (Field field : pojoClass.getDeclaredFields()) {
        builder.add(field.getName(), convertField(typeFactory, field.getType()));
      }
    } else {
      throw new RuntimeException("Either fieldMapping or pojoClass needs to be set.");
    }

    return builder.build();
  }

  private RelDataType convertField(RelDataTypeFactory typeFactory, Class<?> type)
  {
    RelDataType relDataType;

    if ((type == Boolean.class) || (type == boolean.class)) {
      relDataType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    } else if ((type == Double.class) || (type == double.class)) {
      relDataType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
    } else if ((type == Integer.class) || (type == int.class)) {
      relDataType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    } else if ((type == Float.class) || (type == float.class)) {
      relDataType = typeFactory.createSqlType(SqlTypeName.FLOAT);
    } else if ((type == Long.class) || (type == long.class)) {
      relDataType = typeFactory.createSqlType(SqlTypeName.BIGINT);
    } else if ((type == Short.class) || (type == short.class)) {
      relDataType = typeFactory.createSqlType(SqlTypeName.SMALLINT);
    } else if ((type == Character.class) || (type == char.class) || (type == Byte.class) || (type == byte.class)) {
      relDataType = typeFactory.createSqlType(SqlTypeName.CHAR);
    } else if (type == String.class) {
      relDataType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    } else if (type == Date.class) {
      relDataType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
    } else {
      relDataType = typeFactory.createSqlType(SqlTypeName.ANY);
    }
    return relDataType;
  }
}
