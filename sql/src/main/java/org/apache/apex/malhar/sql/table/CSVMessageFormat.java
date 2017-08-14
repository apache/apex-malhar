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

import org.apache.apex.malhar.contrib.formatter.CsvFormatter;
import org.apache.apex.malhar.contrib.parser.CsvParser;
import org.apache.apex.malhar.contrib.parser.DelimitedSchema;
import org.apache.apex.malhar.sql.operators.OperatorUtils;
import org.apache.apex.malhar.sql.planner.RelInfo;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;

@InterfaceStability.Evolving
/**
 * @since 3.6.0
 */
public class CSVMessageFormat implements MessageFormat
{
  public static final String CSV_SCHEMA = "schema";
  private Map<String, Object> operands;

  public CSVMessageFormat()
  {
  }

  public CSVMessageFormat(String schema)
  {
    this.operands = ImmutableMap.<String, Object>of(CSV_SCHEMA, schema);
  }

  @Override
  public MessageFormatType getMessageFormatType()
  {
    return MessageFormatType.CSV;
  }

  @Override
  public void setMessageFormatOperands(Map<String, Object> operands)
  {
    this.operands = operands;
  }

  @Override
  public RelInfo populateInputDAG(DAG dag, JavaTypeFactory typeFactory)
  {
    CsvParser csvParser = dag.addOperator(OperatorUtils.getUniqueOperatorName("CSVParser"), CsvParser.class);
    csvParser.setSchema((String)operands.get(CSV_SCHEMA));

    return new RelInfo("CSVParser", Lists.<Operator.InputPort>newArrayList(csvParser.in), csvParser, csvParser.out,
      getRowType(typeFactory));
  }

  @Override
  public RelInfo populateOutputDAG(DAG dag, JavaTypeFactory typeFactory)
  {
    CsvFormatter formatter = dag.addOperator(OperatorUtils.getUniqueOperatorName("CSVFormatter"), CsvFormatter.class);
    formatter.setSchema((String)operands.get(CSV_SCHEMA));

    return new RelInfo("CSVFormatter", Lists.<Operator.InputPort>newArrayList(formatter.in), formatter, formatter.out,
      getRowType(typeFactory));
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory)
  {
    String schema = (String)operands.get(CSV_SCHEMA);
    RelDataTypeFactory.FieldInfoBuilder builder = typeFactory.builder();

    DelimitedSchema delimitedSchema = new DelimitedSchema(schema);
    for (DelimitedSchema.Field field : delimitedSchema.getFields()) {
      builder.add(field.getName(), convertField(typeFactory, field.getType()));
    }

    return builder.build();
  }

  private RelDataType convertField(RelDataTypeFactory typeFactory, DelimitedSchema.FieldType type)
  {
    RelDataType relDataType;
    switch (type) {
      case BOOLEAN:
        relDataType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
        break;
      case DOUBLE:
        relDataType = typeFactory.createSqlType(SqlTypeName.DOUBLE);
        break;
      case INTEGER:
        relDataType = typeFactory.createSqlType(SqlTypeName.INTEGER);
        break;
      case FLOAT:
        relDataType = typeFactory.createSqlType(SqlTypeName.FLOAT);
        break;
      case LONG:
        relDataType = typeFactory.createSqlType(SqlTypeName.BIGINT);
        break;
      case SHORT:
        relDataType = typeFactory.createSqlType(SqlTypeName.SMALLINT);
        break;
      case CHARACTER:
        relDataType = typeFactory.createSqlType(SqlTypeName.CHAR);
        break;
      case STRING:
        relDataType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
        break;
      case DATE:
        relDataType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
        break;
      default:
        relDataType = typeFactory.createSqlType(SqlTypeName.ANY);
    }

    return relDataType;
  }
}
