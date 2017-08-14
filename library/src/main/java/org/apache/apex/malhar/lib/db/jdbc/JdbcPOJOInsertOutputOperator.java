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
package org.apache.apex.malhar.lib.db.jdbc;

import java.lang.reflect.Field;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashSet;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.util.FieldInfo;

import com.google.common.collect.Lists;

import com.datatorrent.api.Context.OperatorContext;

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
      columnNames = Lists.newArrayList();
      columnDataTypes = Lists.newArrayList();
      columnNullabilities = Lists.newArrayList();
      /**
       * columnNamesSet is the set having column names given by the user
       */
      HashSet<String> columnNamesSet = new HashSet<>();
      if (getFieldInfos() == null || getFieldInfos().size() == 0) { // then assume direct mapping
        LOG.info("FieldInfo missing. Assuming direct mapping between POJO fields and DB columns");
      } else {
        // FieldInfo supplied by user
        StringBuilder columns = new StringBuilder();
        StringBuilder values = new StringBuilder();
        for (int i = 0; i < getFieldInfos().size(); i++) {
          String columnName = getFieldInfos().get(i).getColumnName();
          columns.append(columnName);
          values.append("?");
          if (i < getFieldInfos().size() - 1) {
            columns.append(",");
            values.append(",");
          }
          columnNamesSet.add(columnName.toUpperCase());
        }
      }
      populateColumnDataTypes(columnNamesSet);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void activate(OperatorContext context)
  {
    if (getFieldInfos() == null || getFieldInfos().size() == 0) {
      Field[] fields = pojoClass.getDeclaredFields();
      // Create fieldInfos in case of direct mapping
      List<JdbcFieldInfo> fieldInfos = Lists.newArrayList();
      for (int i = 0; i < columnNames.size(); i++) {
        String columnName = columnNames.get(i);
        String pojoField = getMatchingField(fields, columnName);

        if (columnNullabilities.get(i) == DatabaseMetaData.columnNoNulls &&
            (pojoField == null || pojoField.length() == 0)) {
          throw new RuntimeException("Data for a non-nullable field: " + columnName + " not found in POJO");
        } else {
          if (pojoField != null && pojoField.length() != 0) {
            JdbcFieldInfo fi = new JdbcFieldInfo(columnName, pojoField, null, Types.NULL);
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
      columnFieldGetters.add(new ActiveFieldInfo(fi));
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
      if (f.getName().equalsIgnoreCase(columnName)) {
        return f.getName();
      }
    }
    return null;
  }

  /**
   * Function to populate Meta Data.
   * @param columnNamesSet is a set having column names given by the user
   * @throws SQLException
   */
  protected void populateColumnDataTypes(HashSet<String> columnNamesSet) throws SQLException
  {
    ResultSet rsColumns;
    DatabaseMetaData meta = store.getConnection().getMetaData();
    rsColumns = meta.getColumns(null, null, getTablename(), null);
    /**Identifiers (table names, column names etc.) may be stored internally in either uppercase or lowercase.**/
    if (!rsColumns.isBeforeFirst()) {
      rsColumns = meta.getColumns(null, null, getTablename().toUpperCase(), null);
      if (!rsColumns.isBeforeFirst()) {
        rsColumns = meta.getColumns(null, null, getTablename().toLowerCase(), null);
        if (!rsColumns.isBeforeFirst()) {
          throw new RuntimeException("Table name not found");
        }
      }
    }
    boolean readAllColumns = columnNamesSet.size() == 0 ? true : false;
    int remainingColumns = columnNamesSet.size();
    while (rsColumns.next()) {
      if (readAllColumns || remainingColumns > 0) {
        if (readAllColumns || columnNamesSet.contains(rsColumns.getString("COLUMN_NAME").toUpperCase())) {
          columnNames.add(rsColumns.getString("COLUMN_NAME"));
          columnNullabilities.add(rsColumns.getInt("NULLABLE"));
          columnDataTypes.add(rsColumns.getInt("DATA_TYPE"));
          remainingColumns--;
        }
      } else {
        break;
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
