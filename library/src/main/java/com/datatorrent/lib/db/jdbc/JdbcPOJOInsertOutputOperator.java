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

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.util.FieldInfo;

/**
 * <p>
 * JdbcPOJOInsertOutputOperator class.</p>
 * An implementation of AbstractJdbcTransactionableOutputOperator which takes in any POJO.
 *
 * @displayName Jdbc Output Operator
 * @category Output
 * @tags database, sql, pojo, jdbc
 * @since 2.1.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class JdbcPOJOInsertOutputOperator extends AbstractJdbcPOJOOutputOperator
{
  String insertStatement;
  List<String> columnNames;
  List<Integer> columnNullabilities;
  String columnString;
  String valueString;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);

    // Populate columnNames and columnDataTypes
    try {
      if (getFieldInfos() == null) { // then assume direct mapping
        LOG.info("Assuming direct mapping between POJO fields and DB columns");
        populateColumnDataTypes(null);
      } else {
        // FieldInfo supplied by user
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();
        for (int i = 0; i < getFieldInfos().size(); i++) {
          columns.append(getFieldInfos().get(i).getColumnName());
          values.append("?");
          if (i < getFieldInfos().size() - 1) {
            columns.append(",");
            values.append(",");
          }
        }
        populateColumnDataTypes(columns.toString());
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void activate(OperatorContext context)
  {
    if(getFieldInfos() == null) {
      Field[] fields = pojoClass.getDeclaredFields();
      // Create fieldInfos in case of direct mapping
      List<FieldInfo> fieldInfos = Lists.newArrayList();
      for (int i = 0; i < columnNames.size(); i++) {
        String columnName = columnNames.get(i);
        String pojoField = getMatchingField(fields, columnName);

        if(columnNullabilities.get(i) == ResultSetMetaData.columnNoNulls &&
                (pojoField == null || pojoField.length() == 0)) {
          throw new RuntimeException("Data for a non-nullable field not found in POJO");
        } else {
          if(pojoField != null && pojoField.length() != 0) {
            FieldInfo fi = new FieldInfo(columnName, pojoField, null);
            fieldInfos.add(fi);
          } else {
            columnDataTypes.remove(i);
            columnNames.remove(i);
            columnNullabilities.remove(i);
            i--;
          }
        }
      }
      setFieldInfos(fieldInfos);
    }

    for (FieldInfo fi : getFieldInfos()) {
      columnFieldGetters.add(new JdbcPOJOInputOperator.ActiveFieldInfo(fi));
    }

    StringBuilder columns = new StringBuilder();
    StringBuilder values = new StringBuilder();

    for (int i = 0; i < columnNames.size(); i++) {
      columns.append(columnNames.get(i));
      values.append("?");
      if (i < columnNames.size() - 1) {
        columns.append(",");
        values.append(",");
      }
    }

    insertStatement = "INSERT INTO "
            + getTablename()
            + " (" + columns.toString() + ")"
            + " VALUES (" + values.toString() + ")";
    LOG.debug("insert statement is {}", insertStatement);

    super.activate(context);
  }

  private String getMatchingField(Field[] fields, String columnName)
  {
    for (Field f: fields) {
      if(f.getName().equalsIgnoreCase(columnName)) {
        return f.getName();
      }
    }
    return null;
  }

  protected void populateColumnDataTypes(String columns) throws SQLException
  {
    columnNames = Lists.newArrayList();
    columnDataTypes = Lists.newArrayList();
    columnNullabilities = Lists.newArrayList();

    try (Statement st = store.getConnection().createStatement()) {
      if (columns == null || columns.length() == 0) {
        columns = "*";
      }
      ResultSet rs = st.executeQuery("select " + columns + " from " + getTablename());

      ResultSetMetaData rsMetaData = rs.getMetaData();
      LOG.debug("resultSet MetaData column count {}", rsMetaData.getColumnCount());

      for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
        int type = rsMetaData.getColumnType(i);
        String columnName = rsMetaData.getColumnName(i);
        columnNames.add(columnName);
        columnDataTypes.add(type);
        columnNullabilities.add(rsMetaData.isNullable(i));
        LOG.debug("column name {} type {}", rsMetaData.getColumnName(i), type);
      }
    }
  }


  @Override
  protected String getUpdateCommand()
  {
    return insertStatement;
  }

  private static final Logger LOG = LoggerFactory.getLogger(JdbcPOJOInsertOutputOperator.class);
}
