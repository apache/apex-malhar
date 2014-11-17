package com.datatorrent.demos.goldengate;

import com.datatorrent.api.Context;
import com.datatorrent.common.util.DTThrowable;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by Pramod Immaneni <pramod@datatorrent.com> on 10/22/14.
 */
public class FileQueryProcessor extends QueryProcessor
{
  private static final Logger logger = LoggerFactory.getLogger(FileQueryProcessor.class);
  private static final String GET_LATEST_FILE_CONTENTS = "GET_LATEST_FILE_CONTENTS";

  private static final String CONTENT_DATA = "CONTENT";

  private String filePath;

  private transient FileSystem fs;

  public String getFilePath()
  {
    return filePath;
  }

  public void setFilePath(String filePath)
  {
    this.filePath = filePath;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
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
  }

  @Override
  protected Class<? extends Query> getQueryClass(String selector, JsonNode json)
  {
    logger.info("JSON {}", json);
    Class<? extends Query> queryClass = null;
    if (selector.equals(GET_LATEST_FILE_CONTENTS)) {
      queryClass = GetLatestFileContentsQuery.class;
    }
    return queryClass;
  }

  @Override
  protected void executeQuery(Query query, QueryResults results)
  {
    if (query instanceof GetLatestFileContentsQuery) {
      processGetLatestFileContents((GetLatestFileContentsQuery)query, results);
    }
  }

  public void processGetLatestFileContents(GetLatestFileContentsQuery query, QueryResults results) {
    logger.info("File contents query info {} {}", query.filePath, query.numberLines);
    String filePath = query.filePath;
    if (filePath == null) filePath = this.filePath;
    int numberLines = query.numberLines;
    BufferedReader reader = null;
    try {
      EvictingQueue<String> queue = EvictingQueue.create(numberLines);
      FSDataInputStream inputStream = fs.open(new Path(filePath));
      reader = new BufferedReader(new InputStreamReader(inputStream));
      String line = null;
      while ((line = reader.readLine()) != null) {
        queue.add(line);
      }
      ContentData contentData = new ContentData();
      contentData.lines = queue.toArray(new String[0]);
      results.setData(contentData);
      results.setType(CONTENT_DATA);
      logger.info("result lines {}", contentData.lines.length);
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    } finally {
      if (reader != null) {
        try {
          reader.close();
        } catch (IOException e) {
          logger.error("Error closing reader", e);
        }
      }
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

  public static class ContentData implements QueryResults.Data
  {
    public String[] lines;
  }

}
