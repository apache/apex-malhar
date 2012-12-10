/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.Operator;
import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class JDBCArrayListOutputOperator implements Operator
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCArrayListOutputOperator.class);
  private static final String URL = "url";
  private static final String DB_NAME = "dbName";
  private static final String USER = "user";
  private static final String PASSWORD = "password";
  private static final String DB_DRIVER = "dbDriver";
  private static final String TABLE_NAME = "tableName";
  private static final String COLUMN_MAPPING = "columnMapping";
  private Properties prop = null;
  private Connection connection = null;
  private PreparedStatement ps = null;
  private String tableName;
  private static int count = 0; // for debugging
  private HashMap<String, String> keyToColumn = new HashMap<String, String>();
  private String insertQuery;
  private boolean queryCreated = false;

  public Properties getProp()
  {
    return prop;
  }

  public Connection getConnection()
  {
    return connection;
  }
  /**
   * The input port.
   */
  @InputPortFieldAnnotation(name = "inputPort")
  public final transient DefaultInputPort<ArrayList<AbstractMap.SimpleEntry<String, Object>>> inputPort = new DefaultInputPort<ArrayList<AbstractMap.SimpleEntry<String, Object>>>(this)
  {
    @Override
    public void process(ArrayList<AbstractMap.SimpleEntry<String, Object>> tuple)
    {
      try {
        if (!queryCreated) {
          insertQuery = createInsertStatement(tuple);
          ps = connection.prepareStatement(insertQuery);
          queryCreated = true;
        }

        int num = tuple.size();
        for (int idx=0; idx<num; idx++) {
          ps.setString(idx+1, tuple.get(idx).getValue().toString());
          count++;
        }
        ps.executeUpdate();

      }
      catch (SQLException ex) {
        logger.debug("exception while update", ex);
      }

      logger.debug(String.format("count %d", count));
    }

    /**
     * Fill up column names from tuple
     *
     * @param tuple
     * @return insert query string
     */
    private String createInsertStatement(ArrayList<AbstractMap.SimpleEntry<String, Object>> tuple)
    {
      int num = tuple.size();
      if (num < 1) {
        return null;
      }
      String insertQuery;
      String columns = "";
      String values = "";
      String space = " ";
      String comma = ",";
      String question = "?";

      for (int idx=0; idx<num; idx++) {
        if (idx == 0) {
          columns = keyToColumn.get(tuple.get(idx).getKey());
          values = question;
        }
        else {
          columns = columns + comma + space + keyToColumn.get(tuple.get(idx).getKey());
          values = values + comma + space + question;
        }
      }

      insertQuery = "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + values + ")";
      logger.debug(String.format("%s", insertQuery));
      return insertQuery;
    }
  };

  public void readConfig() throws Exception
  {
    try {
      // Read properties from file
      prop = new Properties();
      prop.load(new FileInputStream("target/classes/com/malhartech/lib/io/DBOperatorDataMapping.properties"));
      tableName = prop.getProperty(TABLE_NAME);
    }
    catch (IOException ex) {
      logger.debug("exception during reading config file", ex);
    }

    // ColumnMapping
    String all = prop.getProperty(COLUMN_MAPPING);
    String[] columns = all.split(",");
    int numColumns = columns.length;
    if (numColumns < 1) {
      throw new Exception("Columns are not separated by ,");
    }

    int idx = 1;  // Assume the columns are in order
    for (String col: columns) {
      String[] cols = col.split(":");
      keyToColumn.put(cols[0], cols[1]);
      idx++;
    }
    logger.debug(keyToColumn.toString());
  }

  /**
   * Implement Component Interface.
   *
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    try {
      readConfig();
    }
    catch (Exception ex) {
      logger.debug("exception during setup", ex);
    }

    try {
      // This will load the MySQL driver, each DB has its own driver
      Class.forName(prop.getProperty(DB_DRIVER)).newInstance();

      connection = DriverManager.getConnection(
              prop.getProperty(URL)
              + prop.getProperty(DB_NAME),
              prop.getProperty(USER),
              prop.getProperty(PASSWORD));
      logger.debug("JDBC connection Success");
    }
    catch (ClassNotFoundException ex) {
      logger.debug("exception during setup", ex);
    }
    catch (Exception ex) {
      logger.debug("exception during setup", ex);
    }
  }

  /**
   * Implement Component Interface.
   */
  @Override
  public void teardown()
  {
    try {
      if (ps != null) {
        ps.close();
      }
      connection.close();
    }
    catch (SQLException ex) {
      logger.debug("exception during teardown", ex);
    }
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void beginWindow(long windowId)
  {
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void endWindow()
  {
  }
}
