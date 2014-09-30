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

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

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
  private final Properties connectionProps;

  protected transient Connection connection = null;

  public JdbcStore()
  {
    connectionProps = new Properties();
  }

  @NotNull
  public String getDbUrl()
  {
    return dbUrl;
  }

  public void setDbUrl(@NotNull String dbUrl)
  {
    logger.debug("setDbUrl {}:", dbUrl);
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
   * @param userName user name
   * @deprecated use {@link #setConnectionProperties(String)} instead.
   */
  public void setUserName(String userName)
  {
    connectionProps.put("user", userName);
  }

  /**
   * Sets the password.
   *
   * @param password password
   * @deprecated use {@link #setConnectionProperties(String)} instead.
   */
  public void setPassword(String password)
  {
    connectionProps.put("password", password);
  }

  /**
   * Sets the properties on the jdbc connection.
   *
   * @param connectionProperties comma separated list of properties. property key and value are separated by colon.
   *                             eg. user:xyz,password:ijk
   */
  public void setConnectionProperties(String connectionProperties)
  {
    String[] properties = Iterables.toArray(Splitter.on(CharMatcher.anyOf(":,")).omitEmptyStrings().trimResults().split(connectionProperties), String.class);
    for (int i = 0; i < properties.length; i += 2) {
      if (i + 1 < properties.length) {
        connectionProps.put(properties[i], properties[i + 1]);
      }
    }
  }

  public Properties getConnectionProps()
  {
    return connectionProps;
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
