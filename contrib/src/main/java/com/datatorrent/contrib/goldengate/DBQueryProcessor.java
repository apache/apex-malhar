package com.datatorrent.contrib.goldengate;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.*;

import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.db.jdbc.JdbcStore;

import com.datatorrent.api.Context;

import com.datatorrent.common.util.DTThrowable;

/**
 * Created by Pramod Immaneni <pramod@datatorrent.com> on 10/21/14.
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
  protected Class<? extends Query> getQueryClass(JsonNode json)
  {
    logger.info("JSON {}", json);
    Class<? extends Query> queryClass = null;
    String selector = json.get("selector").getTextValue();
    if (selector != null) {
      if (selector.equals(GET_RECENT_TABLE_ENTRIES)) {
        queryClass = GetRecentTableEntriesQuery.class;
      }
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
