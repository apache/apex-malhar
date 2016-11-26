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
package org.apache.apex.malhar.sql.schema;

import java.util.Map;

import org.apache.apex.malhar.sql.table.Endpoint;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.StreamableTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.collect.ImmutableList;

/**
 * This is representation of Apex source/destination to Calcite's {@link StreamableTable} table.
 * Any table that gets registered with {@link org.apache.apex.malhar.sql.SQLExecEnvironment}
 * gets registered as {@link ApexSQLTable}.
 *
 * @since 3.6.0
 */
@InterfaceStability.Evolving
public class ApexSQLTable implements ScannableTable, StreamableTable
{
  private SchemaPlus schema;
  private String name;
  private Map<String, Object> operands;
  private RelDataType rowType;
  private Endpoint endpoint;

  public ApexSQLTable(SchemaPlus schemaPlus, String name, Map<String, Object> operands, RelDataType rowType,
      Endpoint endpoint)
  {
    this.schema = schemaPlus;
    this.name = name;
    this.operands = operands;
    this.rowType = rowType;
    this.endpoint = endpoint;
  }

  public ApexSQLTable(SchemaPlus schema, String name, Endpoint endpoint)
  {
    this(schema, name, null, null, endpoint);
  }

  @Override
  public Enumerable<Object[]> scan(DataContext dataContext)
  {
    return null;
  }

  @Override
  public Table stream()
  {
    return new ApexSQLTable(schema, name, operands, rowType, endpoint);
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory)
  {
    if (rowType == null) {
      rowType = endpoint.getRowType(relDataTypeFactory);
    }
    return rowType;
  }

  @Override
  public Statistic getStatistic()
  {
    return Statistics.of(100d, ImmutableList.<ImmutableBitSet>of(), RelCollations.createSingleton(0));
  }

  @Override
  public Schema.TableType getJdbcTableType()
  {
    return Schema.TableType.STREAM;
  }

  public SchemaPlus getSchema()
  {
    return schema;
  }

  public String getName()
  {
    return name;
  }

  public Map<String, Object> getOperands()
  {
    return operands;
  }

  public RelDataType getRowType()
  {
    return rowType;
  }

  public Endpoint getEndpoint()
  {
    return endpoint;
  }
}
