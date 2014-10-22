package com.datatorrent.contrib.goldengate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.common.cache.*;
import com.google.common.collect.EvictingQueue;

import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.datatorrent.lib.db.jdbc.JdbcStore;

import com.datatorrent.api.Context;

import com.datatorrent.common.util.DTThrowable;

/**
 * Created by Pramod Immaneni <pramod@datatorrent.com> on 10/21/14.
 */
public class GoldenGateQueryProcessor extends QueryProcessor implements RemovalListener<String, PreparedStatement>
{
  private static final Logger logger = LoggerFactory.getLogger(GoldenGateQueryProcessor.class);
  private static final String GET_RECENT_TABLE_ENTRIES = "GET_RECENT_TABLE_ENTRIES";
  private static final String GET_LATEST_FILE_CONTENTS = "GET_LATEST_FILE_CONTENTS";

  private static final String TABLE_DATA = "TABLE";
  private static final String CONTENT_DATA = "CONTENT";

  private static final String[] TABLE_HEADERS = {"Employee ID", "Name", "Department"};

  private String getQuery = "select * from %s order by eid limit ?";

  private String filePath;

  protected JdbcStore store;

  private transient FileSystem fs;
  private transient LoadingCache<String, PreparedStatement> statements;
  private int statementSize = 100;

  public GoldenGateQueryProcessor()
  {
    store = new JdbcStore();
  }

  public JdbcStore getStore()
  {
    return store;
  }

  public void setStore(JdbcStore store)
  {
    this.store = store;
  }

  public int getStatementSize()
  {
    return statementSize;
  }

  public void setStatementSize(int statementSize)
  {
    this.statementSize = statementSize;
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
        return store.getConnection().prepareStatement(getTableQuery);
      }
    });
    try {
      fs = FileSystem.get(new Configuration());
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void teardown()
  {
    try {
      fs.close();
    } catch (IOException e) {
      logger.error("Error closing filesystem", e);
    }
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

  public String getFilePath()
  {
    return filePath;
  }

  public void setFilePath(String filePath)
  {
    this.filePath = filePath;
  }

  @Override
  protected Class<? extends Query> getQueryClass(JsonNode json)
  {
    logger.info("JSON {}", json);
    try {
      GetRecentTableEntriesQuery getQuery = new GetRecentTableEntriesQuery();
      String s = mapper.writeValueAsString(getQuery);
      logger.info("Query as JSON {}", s);
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
    Class<? extends Query> queryClass = null;
    String selector = json.get("selector").getTextValue();
    if (selector != null) {
      if (selector.equals(GET_RECENT_TABLE_ENTRIES)) {
        queryClass = GetRecentTableEntriesQuery.class;
      } else if (selector.equals(GET_LATEST_FILE_CONTENTS)) {
        queryClass = GetLatestFileContentsQuery.class;
      }
    }
    return queryClass;
  }

  @Override
  protected void executeQuery(Query query, QueryResults results)
  {
    if (query instanceof GetRecentTableEntriesQuery) {
      processGetRecentTableEntries((GetRecentTableEntriesQuery)query, results);
    } else if (query instanceof GetLatestFileContentsQuery) {
      processGetLatestFileContents((GetLatestFileContentsQuery)query, results);
    }
  }

  public void processGetRecentTableEntries(GetRecentTableEntriesQuery query, QueryResults results) {
    logger.info("Query info {} {}", query.tableName, query.numberEntries);
    String tableName = query.tableName;
    int numberEntries = query.numberEntries;
    try {
      PreparedStatement getStatement = statements.get(tableName);
      getStatement.setInt(1, numberEntries);
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
      resultsData.rows = rows.toArray(resultsData.rows);
      results.setData(resultsData);
      results.setType(TABLE_DATA);
    } catch (SQLException e) {
      DTThrowable.rethrow(e);
    } catch (ExecutionException e) {
      e.printStackTrace();
    }
  }

  public void processGetLatestFileContents(GetLatestFileContentsQuery query, QueryResults results) {
    String filePath = query.filePath;
    if (filePath == null) filePath = this.filePath;
    int numberLines = query.numberLines;
    try {
      EvictingQueue<String> queue = EvictingQueue.create(numberLines);
      FSDataInputStream inputStream = fs.open(new Path(filePath));
      BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
      String line = null;
      while ((line = reader.readLine()) != null) {
        queue.add(line);
      }
      ContentData contentData = new ContentData();
      contentData.lines = queue.toArray(contentData.lines);
      results.setData(contentData);
      results.setType(CONTENT_DATA);
      inputStream.close();
    } catch (IOException e) {
      DTThrowable.rethrow(e);
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

  public static class GetLatestFileContentsQuery extends Query
  {
    public String filePath;
    public int numberLines;

    @Override
    public String toString()
    {
      return "GetLatestFileContentsQuery{" +
              "fileName='" + filePath + '\'' +
              ", numberLines=" + numberLines +
              '}';
    }
  }
  public static class TableData implements QueryResults.Data
  {
    public String[] headers;
    public Object[][] rows;
  }

  public static class ContentData implements QueryResults.Data
  {
    public String[] lines;
  }

}
