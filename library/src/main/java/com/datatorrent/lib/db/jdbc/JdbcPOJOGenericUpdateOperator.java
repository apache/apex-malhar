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
package com.datatorrent.lib.db.jdbc;

import java.io.IOException;
import java.lang.reflect.Field;
import java.sql.Types;
import java.util.Map;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

import org.apache.hadoop.classification.InterfaceStability.Evolving;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.util.FieldInfo;

/**
 * <p>
 * JdbcPOJOUpdateOperator class.
 * </p>
 * A Generic extension of JdbcPOJOOutputOperator which takes in any POJO. This class accepts a parameterized sql query
 * which has "?" as placeholders for values which are dynamically substituted from the incoming POJO.
 * In addition, it also accepts a json string as configuration which has the following structure:
 * [
 *  {
 *    input: pojo field expression,
 *    dataType: sql type of pojo expression,
 *    columnName: name of target column
 *  },
 *  {
 *    ...
 *  }
 * ]
 *
 * @displayName Jdbc Output Operator
 * @category Output
 * @tags database, sql, pojo, jdbc
 */
@Evolving
public class JdbcPOJOGenericUpdateOperator extends JdbcPOJOOutputOperator
{
  private Map<String, Integer> jdbcTypes;

  /**
   * Has the following structure:
   * [
   *  {
   *    input: pojo field expression,
   *    dataType: sql type of pojo expression,
   *    columnName: name of target column
   *  },
   *  {
   *    ...
   *  }
   * ]
   */
  private String updateConfig;
  private String sqlQuery;

  @AutoMetric
  private long numRecordsWritten;
  @AutoMetric
  private long numErrorRecords;

  public transient DefaultOutputPort<Object> error = new DefaultOutputPort<>();

  public JdbcPOJOGenericUpdateOperator()
  {
    jdbcTypes = Maps.newHashMap();
    columnDataTypes = Lists.newArrayList();
    fieldInfos = Lists.newArrayList();
  }

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
    numRecordsWritten = 0;
    numErrorRecords = 0;
  }

  @Override
  public void setup(OperatorContext context)
  {
    for (Field f : Types.class.getFields()) {
      try {
        jdbcTypes.put(f.getName(), f.getInt(null));
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    // Parse json and identify parameter type and pojo expressions
    ObjectMapper mapper = new ObjectMapper();
    try {
      JsonNode configArray = mapper.readTree(updateConfig);
      for (int i = 0; i < configArray.size(); i++) {
        FieldInfo fi = new FieldInfo();
        JsonNode node = configArray.get(i);
        fi.setPojoFieldExpression(node.get("input").asText());
        fi.setColumnName(node.get("columnName").asText());
        columnDataTypes.add(jdbcTypes.get(node.get("dataType").asText()));
        fieldInfos.add(fi);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    super.setup(context);
  }

  @Override
  public void processTuple(Object tuple)
  {
    try {
      super.processTuple(tuple);
      numRecordsWritten++;
    } catch (RuntimeException e) {
      numErrorRecords++;
      error.emit(tuple);
    }
  }

  @Override
  protected String getUpdateCommand()
  {
    return sqlQuery;
  }

  /**
   * Sets the parameterized SQL query for the JDBC update operation.
   * This can be an insert, update, delete or a merge query.
   * Example: "update testTable set id = ? where name = ?"
   * @param sqlQuery the query statement
   */
  protected void setUpdateCommand(String sqlQuery)
  {
    this.sqlQuery = sqlQuery;
  }

  public String getUpdateConfigJsonString()
  {
    return updateConfig;
  }

  /**
   * Set the config for parameters in the SQL query.
   * The string is a valid JSON array.
   * Example:
   * [
   *  {
   *    "input": "id",
   *    "dataType": "INTEGER",
   *    "columnName": "id"
   *  },
   *  {
   *    ...
   *  }
   * ]
   * @param updateConfigJsonString the update string
   */
  public void setUpdateConfigJsonString(String updateConfigJsonString)
  {
    this.updateConfig = updateConfigJsonString;
  }
}
