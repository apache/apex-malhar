/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.Operator;
import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Locknath Shil <locknath@malhar-inc.com>
 */
public class JDBCNonTransactionOutputOperator<V> implements Operator
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCNonTransactionOutputOperator.class);
  private String dbUrl;
  private String dbName;
  private String dbUser;
  private String dbPassword;
  private String dbDriver;
  private String tableName;
  private ArrayList<String> orderedColumnMapping = new ArrayList<String>();
  private ArrayList<String> orderedColumns = new ArrayList<String>(); // follow same order as items in tuple
  private HashMap<String, Integer> keyToIndex = new HashMap<String, Integer>();
  private HashMap<String, String> keyToType = new HashMap<String, String>();
  private HashMap<String, String> keyToColumn = new HashMap<String, String>(); // only used for debugging
  private Properties prop = null;
  private Connection connection = null;
  private PreparedStatement ps = null;
  private static int count = 0; // for debugging
  protected Statement statement;
  protected long windowId;
  protected long lastWindowId;

  public String getDbUrl()
  {
    return dbUrl;
  }

  public void setDbUrl(String dbUrl)
  {
    this.dbUrl = dbUrl;
  }

  public String getDbName()
  {
    return dbName;
  }

  public void setDbName(String dbName)
  {
    this.dbName = dbName;
  }

  public String getDbUser()
  {
    return dbUser;
  }

  public void setDbUser(String dbUser)
  {
    this.dbUser = dbUser;
  }

  public String getDbPassword()
  {
    return dbPassword;
  }

  public void setDbPassword(String dbPassword)
  {
    this.dbPassword = dbPassword;
  }

  public String getDbDriver()
  {
    return dbDriver;
  }

  public void setDbDriver(String dbDriver)
  {
    this.dbDriver = dbDriver;
  }

  public String getTableName()
  {
    return tableName;
  }

  public void setTableName(String tableName)
  {
    this.tableName = tableName;
  }

  public ArrayList<String> getOrderedColumnMapping()
  {
    return orderedColumnMapping;
  }

  public void setOrderedColumnMapping(String[] orderedColumnMapping)
  {
    if (orderedColumnMapping != null) {
      this.orderedColumnMapping.addAll(Arrays.asList(orderedColumnMapping));
    }
  }

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
  public final transient DefaultInputPort<HashMap<String, V>> inputPort = new DefaultInputPort<HashMap<String, V>>(this)
  {
    @Override
    public void process(HashMap<String, V> tuple)
    {
      if( windowId <= lastWindowId )  { // stale data
        return;
      }
      try {
        for (Map.Entry<String, V> e : tuple.entrySet()) {
          ps.setString(keyToIndex.get(e.getKey()).intValue(), e.getValue().toString());
          count++;
        }
        ps.setString(tuple.size()+1, dbUrl);
        ps.executeUpdate();
      }
      catch (SQLException ex) {
        logger.debug("exception while update", ex);
      }

      logger.debug(String.format("count %d", count));
    }
  };

  public void buildMapping()
  {
    try {
      // Each entry in orderedColumnMapping is Tuple key followed by Tuple value separated by colon (:)
      int num = orderedColumnMapping.size();
      String delimiter = ":";

      for (int idx = 0; idx < num; ++idx) {
        String[] cols = orderedColumnMapping.get(idx).split(delimiter);
        if (cols.length < 2 || cols.length > 3) {
          throw new Exception("bad column mapping");
        }
        keyToColumn.put(cols[0], cols[1]);
        keyToIndex.put(cols[0], new Integer(idx + 1));
        orderedColumns.add(cols[1]);
        keyToType.put(cols[0], (cols.length == 3) ? cols[2] : "UNSPECIFIED");
      }
      logger.debug(keyToColumn.toString());
    }
    catch (Exception ex) {
      logger.debug("exception during table column mapping", ex);
    }
  }

  public void setupJDBCConnection()
  {
    try {
      // This will load the MySQL driver, each DB has its own driver
      Class.forName(dbDriver).newInstance();
      connection = DriverManager.getConnection(dbUrl + dbName, dbUser, dbPassword);
      logger.debug("JDBC connection Success");
    }
    catch (ClassNotFoundException ex) {
      logger.debug("exception during JBDC connection", ex);
    }
    catch (Exception ex) {
      logger.debug("exception during JDBC connection", ex);
    }
  }

  /**
   * Prepare insert query statement using column names from mapping.
   *
   */
  private void prepareInsertStatement()
  {
    int num = orderedColumns.size();
    if (num < 1) {
      return;
    }
    String columns = "";
    String values = "";
    String space = " ";
    String comma = ",";
    String question = "?";

    for (int idx = 0; idx < num; ++idx) {
      if (idx == 0) {
        columns = orderedColumns.get(idx);
        values = question;
      }
      else {
        columns = columns + comma + space + orderedColumns.get(idx);
        values = values + comma + space + question;
      }
    }
    columns += columns + comma + space +"winid";
    values = values + comma + space + question;

    String insertQuery = "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + values + ")";
    logger.debug(String.format("%s", insertQuery));
    try {
      ps = connection.prepareStatement(insertQuery);
    }
    catch (SQLException ex) {
      logger.debug("exception during prepare statement", ex);
    }
  }

  public void initLastWindowInfo(String table)
  {
    try {
      statement = connection.createStatement();
      String stmt = "SELECT MAX(winid) AS winid FROM "+table;
      ResultSet rs = statement.executeQuery(stmt);
      if (rs.next() == false) {
        logger.error("table " + table + " winid column not ready!");
        return;
      }
      lastWindowId = rs.getLong("winid");
    }
    catch (SQLException ex) {
      logger.debug(ex.toString());
    }
  }

  /**
   * Implement Component Interface.
   *
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    buildMapping();
    setupJDBCConnection();
    prepareInsertStatement();
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
    this.windowId = windowId;
    logger.debug("window:" + windowId);

  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void endWindow()
  {
  }
}
