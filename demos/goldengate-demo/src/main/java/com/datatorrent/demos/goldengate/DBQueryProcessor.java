/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.goldengate;

import com.datatorrent.api.Context;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.db.jdbc.JdbcStore;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This operator processes queries on the database containing employee data.
 */
public class DBQueryProcessor extends QueryProcessor implements RemovalListener<String, PreparedStatement>
{
  private static final Logger logger = LoggerFactory.getLogger(DBQueryProcessor.class);
  private static final String GET_RECENT_TABLE_ENTRIES = "GET_RECENT_TABLE_ENTRIES";
  private static final String TABLE_DATA = "TABLE";
  private static final String[] TABLE_HEADERS = {"Employee ID", "Name", "Department"};

  private String getQuery = "select * from (select * from %s order by eid desc) where rownum < ?";

  protected JdbcStore store;
  private transient LoadingCache<String, PreparedStatement> statements;
  private int statementSize = 100;

  public DBQueryProcessor()
  {
    store = new JdbcStore();
  }

  public int getStatementSize()
  {
    return statementSize;
  }

  public void setStatementSize(int statementSize)
  {
    this.statementSize = statementSize;
  }

  public JdbcStore getStore()
  {
    return store;
  }

  public void setStore(JdbcStore store)
  {
    this.store = store;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    store.connect();
    statements = CacheBuilder.newBuilder().maximumSize(statementSize).removalListener(this).build(new CacheLoader<String, PreparedStatement>()
    {
      @Override
      public PreparedStatement load(String s) throws Exception
      {
        String getTableQuery = String.format(getQuery, s);
        logger.info("Get query {}", getTableQuery);
        return store.getConnection().prepareStatement(getTableQuery);
      }
    });
  }

  @Override
  public void teardown()
  {
    statements.cleanUp();
    store.disconnect();
  }

  @Override
  public void onRemoval(RemovalNotification<String, PreparedStatement> notification)
  {
    try {
      notification.getValue().close();
    } catch (SQLException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  protected Class<? extends Query> getQueryClass(String selector, JsonNode json)
  {
    logger.info("Selector {} JSON {}", selector, json);
    Class<? extends Query> queryClass = null;
    if (selector.equals(GET_RECENT_TABLE_ENTRIES)) {
      queryClass = GetRecentTableEntriesQuery.class;
    }
    return queryClass;
  }

  @Override
  protected void executeQuery(Query query, QueryResults results)
  {
    if (query instanceof GetRecentTableEntriesQuery) {
      processGetRecentTableEntries((GetRecentTableEntriesQuery)query, results);
    }
  }

  public void processGetRecentTableEntries(GetRecentTableEntriesQuery query, QueryResults results) {
    logger.info("Get recent entries query info {} {}", query.tableName, query.numberEntries);
    String tableName = query.tableName;
    int numberEntries = query.numberEntries;
    try {
      PreparedStatement getStatement = statements.get(tableName);
      getStatement.setInt(1, numberEntries+1);
      logger.info("query {}", getStatement);
      ResultSet resultSet = getStatement.executeQuery();
      List<Object[]> rows = new ArrayList<Object[]>();
      while (resultSet.next()) {
        Object[] row = new Object[3];
        row[0] = resultSet.getInt(1);
        row[1] = resultSet.getString(2);
        row[2] = resultSet.getInt(3);
        rows.add(row);
      }
      TableData resultsData = new TableData();
      resultsData.headers = TABLE_HEADERS;
      resultsData.rows = rows.toArray(new Object[0][0]);
      results.setData(resultsData);
      results.setType(TABLE_DATA);
      logger.info("result rows {}", resultsData.rows.length);
    } catch (SQLException e) {
      DTThrowable.rethrow(e);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  public static class GetRecentTableEntriesQuery extends Query
  {
    public String tableName;
    public int numberEntries;

    @Override
    public String toString()
    {
      return "GetRecentTableEntriesQuery{" +
              "tableName='" + tableName + '\'' +
              ", numberEntries=" + numberEntries +
              '}';
    }
  }

  public static class TableData implements QueryResults.Data
  {
    public String[] headers;
    public Object[][] rows;
  }

}
