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
package com.datatorrent.lib.db.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.classification.InterfaceStability.Evolving;

import com.datatorrent.lib.util.KeyValPair;

/**
 * A utility class used to retrieve the metadata for a given unique key of a SQL
 * table. This class would emit range queries based on a primary index given
 * 
 * LIMIT clause may not work with all databases or may not return the same
 * results always<br>
 * 
 * This utility has been tested with MySQL and where clause is supported<br>
 * 
 * @Input - dbName,tableName, primaryKey
 * @Output - map<operatorId,prepared statement>
 *
 */
@Evolving
public class JdbcMetaDataUtility
{
  private static String DB_DRIVER = "com.mysql.jdbc.Driver";
  private static String DB_CONNECTION = "";
  private static String DB_USER = "";
  private static String DB_PASSWORD = "";
  private static String TABLE_NAME = "";
  private static String KEY_COLUMN = "";
  private static String WHERE_CLAUSE = null;
  private static String COLUMN_LIST = null;

  private static Logger LOG = LoggerFactory.getLogger(JdbcMetaDataUtility.class);

  public JdbcMetaDataUtility()
  {

  }

  public JdbcMetaDataUtility(String dbConnection, String tableName, String key, String userName, String password)
  {
    DB_CONNECTION = dbConnection;
    DB_USER = userName;
    DB_PASSWORD = password;
    TABLE_NAME = tableName;
    KEY_COLUMN = key;
  }

  /**
   * Returns the database connection handle
   * */
  private static Connection getDBConnection()
  {

    Connection dbConnection = null;

    try {
      Class.forName(DB_DRIVER);
    } catch (ClassNotFoundException e) {
      LOG.error("Driver not found", e);
      throw new RuntimeException(e);
    }

    try {
      dbConnection = DriverManager.getConnection(DB_CONNECTION, DB_USER, DB_PASSWORD);
      return dbConnection;
    } catch (SQLException e) {
      LOG.error("Exception in getting connection handle", e);
      throw new RuntimeException(e);
    }

  }

  private static String generateQueryString()
  {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT COUNT(*) as RowCount from " + TABLE_NAME);

    if (WHERE_CLAUSE != null) {
      sb.append(" WHERE " + WHERE_CLAUSE);
    }

    return sb.toString();
  }

  /**
   * Finds the total number of rows in the table
   */
  private static long getRecordRange(String query) throws SQLException
  {
    long rowCount = 0;
    Connection dbConnection = null;
    PreparedStatement preparedStatement = null;

    try {
      dbConnection = getDBConnection();
      preparedStatement = dbConnection.prepareStatement(query);

      ResultSet rs = preparedStatement.executeQuery();

      while (rs.next()) {
        rowCount = Long.parseLong(rs.getString(1));
        LOG.debug("# Rows - " + rowCount);
      }

    } catch (SQLException e) {
      LOG.error("Exception in retreiving result set", e);
      throw new RuntimeException(e);
    } finally {
      if (preparedStatement != null) {
        preparedStatement.close();
      }
      if (dbConnection != null) {
        dbConnection.close();
      }
    }
    return rowCount;
  }

  /**
   * Returns a pair of <upper,lower> bounds for each partition of the
   * {@link JdbcPollInputOperator}}
   */
  private static KeyValPair<String, String> getQueryBounds(long lower, long upper) throws SQLException
  {
    Connection dbConnection = null;
    PreparedStatement psLowerBound = null;
    PreparedStatement psUpperBound = null;

    StringBuilder lowerBound = new StringBuilder();
    StringBuilder upperBound = new StringBuilder();

    KeyValPair<String, String> boundedQUeryPair = null;

    try {
      dbConnection = getDBConnection();

      /*
       * Run this loop only for n-1 partitions.
       * By default the last partition will have fewer records, since we are rounding off
       * */

      lowerBound.append("SELECT " + KEY_COLUMN + " FROM " + TABLE_NAME);
      upperBound.append("SELECT " + KEY_COLUMN + " FROM " + TABLE_NAME);

      if (WHERE_CLAUSE != null) {
        lowerBound.append(" WHERE " + WHERE_CLAUSE);
        upperBound.append(" WHERE " + WHERE_CLAUSE);
      }

      lowerBound.append(" LIMIT " + (0 + lower) + ",1");
      upperBound.append(" LIMIT " + (upper - 1) + ",1");

      psLowerBound = dbConnection.prepareStatement(lowerBound.toString());
      psUpperBound = dbConnection.prepareStatement(upperBound.toString());

      ResultSet rsLower = psLowerBound.executeQuery();
      ResultSet rsUpper = psUpperBound.executeQuery();

      String lowerVal = null;
      String upperVal = null;

      while (rsLower.next()) {
        lowerVal = rsLower.getString(KEY_COLUMN);
      }

      while (rsUpper.next()) {
        upperVal = rsUpper.getString(KEY_COLUMN);
      }

      boundedQUeryPair = new KeyValPair<String, String>(lowerVal, upperVal);

    } catch (SQLException e) {
      LOG.error("Exception in getting bounds for queries");
      throw new RuntimeException(e);
    } finally {
      if (psLowerBound != null) {
        psLowerBound.close();
      }
      if (psUpperBound != null) {
        psUpperBound.close();
      }
      if (dbConnection != null) {
        dbConnection.close();
      }
    }
    return boundedQUeryPair;

  }

  /**
   * Returns a map of partitionId to a KeyValPair of <lower,upper> of the query
   * range<br>
   * Ensures even distribution of records across partitions except the last
   * partition By default the last partition for batch queries will have fewer
   * records, since we are rounding off
   * 
   * @throws SQLException
   * 
   */
  private static HashMap<Integer, KeyValPair<String, String>> getRangeQueries(int numberOfPartitions, int events,
      long rowCount) throws SQLException
  {
    HashMap<Integer, KeyValPair<String, String>> partitionToQueryMap = new HashMap<Integer, KeyValPair<String, String>>();
    for (int i = 0, lowerOffset = 0, upperOffset = events; i < numberOfPartitions
        - 1; i++, lowerOffset += events, upperOffset += events) {

      partitionToQueryMap.put(i, getQueryBounds(lowerOffset, upperOffset));
    }

    //Call to construct the lower and upper bounds for the last partition
    partitionToQueryMap.put(numberOfPartitions - 1, getQueryBounds(events * (numberOfPartitions - 1), rowCount));

    LOG.info("Partition map - " + partitionToQueryMap.toString());

    return partitionToQueryMap;
  }

  /**
   * Helper function returns a range query based on the bounds passed<br>
   * Invoked from the setup method of {@link - AbstractJdbcPollInputOperator} to
   * initialize the preparedStatement in the given operator<br>
   * Optional whereClause for conditional selections Optional columnList for
   * projection
   */
  public static String buildRangeQuery(String tableName, String keyColumn, String lower, String upper)
  {

    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");
    if (COLUMN_LIST != null) {
      sb.append(COLUMN_LIST);
    } else {
      sb.append("*");
    }
    sb.append(" from " + tableName + " WHERE ");
    if (WHERE_CLAUSE != null) {
      sb.append(WHERE_CLAUSE + " AND ");
    }
    sb.append(keyColumn + " BETWEEN '" + lower + "' AND '" + upper + "'");
    return sb.toString();
  }

  /**
   * Helper function that constructs a query from the next highest key after an
   * operator is restarted
   */
  public static String buildGTRangeQuery(String tableName, String keyColumn, String lower, String upper)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");
    if (COLUMN_LIST != null) {
      sb.append(COLUMN_LIST);
    } else {
      sb.append("*");
    }
    sb.append(" from " + tableName + " WHERE ");
    if (WHERE_CLAUSE != null) {
      sb.append(WHERE_CLAUSE + " AND ");
    }
    sb.append(keyColumn + " > '" + lower + "' AND " + keyColumn + " <= '" + upper + "'");
    return sb.toString();
  }

  /**
   * Helper function that constructs a query for polling from outside the given
   * range
   */
  public static String buildPollableQuery(String tableName, String keyColumn, String lower)
  {
    StringBuilder sb = new StringBuilder();
    sb.append("SELECT ");
    if (COLUMN_LIST != null) {
      sb.append(COLUMN_LIST);
    } else {
      sb.append("*");
    }
    sb.append(" from " + tableName + " WHERE ");
    if (WHERE_CLAUSE != null) {
      sb.append(WHERE_CLAUSE + " AND ");
    }
    sb.append(keyColumn + " > '" + lower + "' ");
    return sb.toString();
  }

  /**
   * Called by the partitioner from {@link - AbstractJdbcPollInputOperator}<br>
   * Finds the range of query per partition<br>
   * Returns a map of partitionId to PreparedStatement based on the range
   * computed
   * 
   * @throws SQLException
   */
  public static HashMap<Integer, KeyValPair<String, String>> getPartitionedQueryMap(int partitions, String dbDriver,
      String dbConnection, String tableName, String key, String userName, String password, String whereClause,
      String columnList) throws SQLException
  {
    long rowCount = 0L;
    try {
      DB_CONNECTION = dbConnection;
      DB_USER = userName;
      DB_PASSWORD = password;
      TABLE_NAME = tableName;
      KEY_COLUMN = key;
      WHERE_CLAUSE = whereClause;
      COLUMN_LIST = columnList;
      DB_DRIVER = dbDriver;
      rowCount = getRecordRange(generateQueryString());
    } catch (SQLException e) {
      LOG.error("Exception in getting the record range", e);
    }
    return getRangeQueries(partitions, getOffset(rowCount, partitions), rowCount);
  }

  /**
   * Returns the rounded offset to arrive at a range query
   */
  private static int getOffset(long rowCount, int partitions)
  {
    if (rowCount % partitions == 0) {
      return (int)(rowCount / partitions);
    } else {
      return (int)((rowCount - (rowCount % (partitions - 1))) / (partitions - 1));
    }
  }
}
