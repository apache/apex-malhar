package com.datatorrent.contrib.goldengate;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

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
public class GoldenGateQueryProcessor extends QueryProcessor
{
  private static final Logger logger = LoggerFactory.getLogger(GoldenGateQueryProcessor.class);
  private static final String GET_RECENT_TABLE_ENTRIES = "GET_RECENT_TABLE_ENTRIES";
  private static final String GET_LATEST_FILE_CONTENTS = "GET_LATEST_FILE_CONTENTS";

  private static final String TABLE_DATA = "TABLE";
  private static final String CONTENT_DATA = "CONTENT";

  private static final String[] TABLE_HEADERS = {"Employee ID", "Name", "Department"};

  private String getQuery = "select * from ? order by eid limit ?";

  private String filePath;

  protected JdbcStore store;

  private transient PreparedStatement getStatement;
  private transient FileSystem fs;

  public GoldenGateQueryProcessor()
  {
    store = new JdbcStore();
    store.setDbDriver("oracle.jdbc.driver.OracleDriver");
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
    try {
      getStatement = store.getConnection().prepareStatement(getQuery);
    } catch (SQLException e) {
      DTThrowable.rethrow(e);
    }
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
    try {
      getStatement.close();
    } catch (SQLException e) {
      logger.error("Error closing statements", e);
    }
    store.disconnect();
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
    String tableName = query.tableName;
    int numberEntries = query.numberEntries;
    try {
      getStatement.setString(1, tableName);
      getStatement.setInt(2, numberEntries);
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
