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
package com.datatorrent.lib.db.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.db.Connectable;

/**
 * A {@link Connectable} that uses jdbc to connect to stores.
 *
 * @since 0.9.4
 */
public class JdbcStore implements Connectable
{
  protected static final Logger logger = LoggerFactory.getLogger(JdbcStore.class);

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

  public void setDbUrl(@NotNull String dbUrl)
  {
    this.dbUrl = dbUrl;
  }

  @NotNull
  public String getDbDriver()
  {
    return dbDriver;
  }

  public void setDbDriver(@NotNull String dbDriver)
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
  public void connect()
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
    catch (Throwable t) {
      DTThrowable.rethrow(t);
    }
  }

  /**
   * Close JDBC connection.
   */
  @Override
  public void disconnect()
  {
    try {
      connection.close();
    }
    catch (SQLException ex) {
      throw new RuntimeException("closing database resource", ex);
    }
  }

  @Override
  public boolean connected()
  {
    try {
      return !connection.isClosed();
    }
    catch (SQLException e) {
      throw new RuntimeException("is connected", e);
    }
  }
}
