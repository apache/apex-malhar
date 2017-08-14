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
package org.apache.apex.malhar.contrib.enrich;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.apex.malhar.lib.db.jdbc.JdbcStore;
import org.apache.apex.malhar.lib.util.FieldInfo;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.classification.InterfaceStability;

import com.google.common.collect.Lists;

/**
 * <p>HBaseLoader extends from {@link JdbcStore} uses JDBC to connect and implements BackendLoaders interface.</p> <br/>
 * <p>
 * Properties:<br>
 * <b>queryStmt</b>: Sql Prepared Statement which needs to be executed<br>
 * <b>tableName</b>: JDBC table name<br>
 * <br>
 *
 * @since 3.4.0
 */
@InterfaceStability.Evolving
public class JDBCLoader extends JdbcStore implements BackendLoader
{
  protected String queryStmt;

  protected String tableName;

  protected transient List<FieldInfo> includeFieldInfo;
  protected transient List<FieldInfo> lookupFieldInfo;

  protected Object getQueryResult(Object key)
  {
    try {
      PreparedStatement getStatement = getConnection().prepareStatement(queryStmt);
      ArrayList<Object> keys = (ArrayList<Object>)key;
      for (int i = 0; i < keys.size(); i++) {
        getStatement.setObject(i + 1, keys.get(i));
      }
      return getStatement.executeQuery();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected ArrayList<Object> getDataFrmResult(Object result) throws RuntimeException
  {
    try {
      ResultSet resultSet = (ResultSet)result;
      if (resultSet.next()) {
        ResultSetMetaData rsdata = resultSet.getMetaData();
        // If the includefields is empty, populate it from ResultSetMetaData
        if (CollectionUtils.isEmpty(includeFieldInfo)) {
          if (includeFieldInfo == null) {
            includeFieldInfo = new ArrayList<>();
          }
          for (int i = 1; i <= rsdata.getColumnCount(); i++) {
            String columnName = rsdata.getColumnName(i);
            // TODO: Take care of type conversion.
            includeFieldInfo.add(new FieldInfo(columnName, columnName, FieldInfo.SupportType.OBJECT));
          }
        }

        ArrayList<Object> res = new ArrayList<Object>();
        for (FieldInfo f : includeFieldInfo) {
          res.add(getConvertedData(resultSet.getObject(f.getColumnName()), f));
        }
        return res;
      } else {
        return null;
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private Object getConvertedData(Object object, FieldInfo f)
  {
    if (f.getType().getJavaType() == object.getClass()) {
      return object;
    } else {
      logger.warn("Type mismatch seen for field {}, returning as it is", f.getColumnName());
      return null;
    }
  }

  private String generateQueryStmt()
  {
    String stmt = "select * from " + tableName + " where ";
    boolean first = true;
    for (FieldInfo fieldInfo : lookupFieldInfo) {
      if (first) {
        first = false;
      } else {
        stmt += " and ";
      }
      stmt += fieldInfo.getColumnName() + " = ?";
    }

    logger.info("generateQueryStmt: {}", stmt);
    return stmt;
  }

  public String getQueryStmt()
  {
    return queryStmt;
  }

  /**
   * Set the sql Prepared Statement if the enrichment mechanism is query based.
   */
  public void setQueryStmt(String queryStmt)
  {
    this.queryStmt = queryStmt;
  }

  public String getTableName()
  {
    return tableName;
  }

  /**
   * Set the table name.
   */
  public void setTableName(String tableName)
  {
    this.tableName = tableName;
  }

  @Override
  public void setFieldInfo(List<FieldInfo> lookupFieldInfo, List<FieldInfo> includeFieldInfo)
  {
    this.lookupFieldInfo = lookupFieldInfo;
    this.includeFieldInfo = includeFieldInfo;
    if ((queryStmt == null) || (queryStmt.length() == 0)) {
      queryStmt = generateQueryStmt();
    }
  }

  @Override
  public Map<Object, Object> loadInitialData()
  {
    return null;
  }

  @Override
  public Object get(Object key)
  {
    return getDataFrmResult(getQueryResult(key));
  }

  @Override
  public List<Object> getAll(List<Object> keys)
  {
    List<Object> values = Lists.newArrayList();
    for (Object key : keys) {
      values.add(get(key));
    }
    return values;
  }

  @Override
  public void put(Object key, Object value)
  {
    throw new UnsupportedOperationException("Not supported operation");
  }

  @Override
  public void putAll(Map<Object, Object> m)
  {
    throw new UnsupportedOperationException("Not supported operation");
  }

  @Override
  public void remove(Object key)
  {
    throw new UnsupportedOperationException("Not supported operation");
  }
}
