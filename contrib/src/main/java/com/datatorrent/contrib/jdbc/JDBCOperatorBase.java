/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.database.DBConnector;

/**
 * Handles JDBC connection parameters and driver.
 *
 * @since 0.3.2
 */
public class JDBCOperatorBase implements DBConnector
{
  @NotNull
  private String dbUrl;
  @NotNull
  private String dbDriver;
  private String userName;
  private String password;
  protected transient Connection connection = null;

  @NotNull
  public String getDbUrl()
  {
    return dbUrl;
  }

  public void setDbUrl(String dbUrl)
  {
    this.dbUrl = dbUrl;
  }

  @NotNull
  public String getDbDriver()
  {
    return dbDriver;
  }

  public void setDbDriver(String dbDriver)
  {
    this.dbDriver = dbDriver;
  }

  public Connection getConnection()
  {
    return connection;
  }

  /**
   * Sets the user name.
   *
   * @param userName user name.
   */
  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  /**
   * Sets the password.
   *
   * @param password password
   */
  public void setPassword(String password)
  {
    this.password = password;
  }

  /**
   * Create connection with database using JDBC.
   */
  @Override
  public void setupDbConnection()
  {
    try {
      // This will load the JDBC driver, each DB has its own driver
      Class.forName(dbDriver).newInstance();
      Properties connectionProps = new Properties();
      if (userName != null) {
        connectionProps.put("user", this.userName);
      }
      if (password != null) {
        connectionProps.put("password", this.password);
      }
      connection = DriverManager.getConnection(dbUrl, connectionProps);

      logger.debug("JDBC connection Success");
    }
    catch (ClassNotFoundException ex) {
      throw new RuntimeException("Exception during JBDC connection", ex);
    }
    catch (Exception ex) {
      throw new RuntimeException("Exception during JBDC connection", ex);
    }
  }

  /**
   * Close JDBC connection.
   */
  @Override
  public void teardownDbConnection()
  {
    try {
      connection.close();
    }
    catch (SQLException ex) {
      throw new RuntimeException("Error while closing database resource", ex);
    }
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof JDBCOperatorBase)) {
      return false;
    }

    JDBCOperatorBase that = (JDBCOperatorBase) o;

    if (dbDriver != null ? !dbDriver.equals(that.dbDriver) : that.dbDriver != null) {
      return false;
    }
    if (dbUrl != null ? !dbUrl.equals(that.dbUrl) : that.dbUrl != null) {
      return false;
    }
    if (password != null ? !password.equals(that.password) : that.password != null) {
      return false;
    }
    if (userName != null ? !userName.equals(that.userName) : that.userName != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = dbUrl != null ? dbUrl.hashCode() : 0;
    result = 31 * result + (dbDriver != null ? dbDriver.hashCode() : 0);
    result = 31 * result + (userName != null ? userName.hashCode() : 0);
    result = 31 * result + (password != null ? password.hashCode() : 0);
    return result;
  }

  private static final Logger logger = LoggerFactory.getLogger(JDBCOperatorBase.class);
}
