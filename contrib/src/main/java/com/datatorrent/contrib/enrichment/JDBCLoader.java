package com.datatorrent.contrib.enrichment;

import com.datatorrent.lib.db.jdbc.JdbcStore;
import com.google.common.collect.Lists;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.commons.collections.CollectionUtils;

/**
 * <p>HBaseLoader extends from {@link JdbcStore} uses JDBC to connect and implements EnrichmentBackup interface.</p> <br/>
 *
 * Properties:<br>
 * <b>queryStmt</b>: Sql Prepared Statement which needs to be executed<br>
 * <b>tableName</b>: JDBC table name<br>
 * <br>
 *
 * @displayName JDBCLoader
 * @tags Loader
 * @since 2.1.0
 */
public class JDBCLoader extends JdbcStore implements EnrichmentBackup
{
  protected String queryStmt;

  protected String tableName;

  protected transient List<String> includeFields;
  protected transient List<String> lookupFields;

  protected Object getQueryResult(Object key)
  {
    try {
      PreparedStatement getStatement = getConnection().prepareStatement(queryStmt);
      ArrayList<Object> keys = (ArrayList<Object>) key;
      for (int i = 0; i < keys.size(); i++) {
        getStatement.setObject(i+1, keys.get(i));
      }
      return getStatement.executeQuery();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  protected ArrayList<Object> getDataFrmResult(Object result) throws RuntimeException
  {
    try {
      ResultSet resultSet = (ResultSet) result;
      if (resultSet.next()) {
        ResultSetMetaData rsdata = resultSet.getMetaData();
        // If the includefields is empty, populate it from ResultSetMetaData
        if(CollectionUtils.isEmpty(includeFields)) {
          if(includeFields == null)
            includeFields = new ArrayList<String>();
          for (int i = 1; i <= rsdata.getColumnCount(); i++) {
            includeFields.add(rsdata.getColumnName(i));
          }
        }
        ArrayList<Object> res = new ArrayList<Object>();
        for(String f : includeFields) {
          res.add(resultSet.getObject(f));
        }
        return res;
      } else
        return null;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private String generateQueryStmt()
  {
    String stmt = "select * from " + tableName + " where ";
    for (int i = 0; i < lookupFields.size(); i++) {
      stmt = stmt + lookupFields.get(i) + " = ? ";
      if(i != lookupFields.size() - 1) {
        stmt = stmt + " and ";
      }
    }
    logger.info("generateQueryStmt: {}", stmt);
    return stmt;
  }

  public String getQueryStmt()
  {
    return queryStmt;
  }

  @Override
  public boolean needRefresh() {
    return false;
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

  @Override public void setFields(List<String> lookupFields,List<String> includeFields)
  {
    this.includeFields = includeFields;
    this.lookupFields = lookupFields;
    if(queryStmt == null)
      queryStmt = generateQueryStmt();
  }
  @Override public Map<Object, Object> loadInitialData()
  {
    return null;
  }

  @Override public Object get(Object key)
  {
    return getDataFrmResult(getQueryResult(key));
  }

  @Override public List<Object> getAll(List<Object> keys)
  {
    List<Object> values = Lists.newArrayList();
    for (Object key : keys) {
      values.add(get(key));
    }
    return values;
  }

  @Override public void put(Object key, Object value)
  {
    throw new RuntimeException("Not supported operation");
  }

  @Override public void putAll(Map<Object, Object> m)
  {
    throw new RuntimeException("Not supported operation");
  }

  @Override public void remove(Object key)
  {
    throw new RuntimeException("Not supported operation");
  }
}
