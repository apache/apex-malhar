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
package org.apache.apex.malhar.lib.db.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.validation.constraints.NotNull;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.db.Connectable;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;

import com.datatorrent.netlet.util.DTThrowable;

/**
 * A {@link Connectable} that uses jdbc to connect to stores.
 *
 * @since 0.9.4
 */
public class JdbcStore implements Connectable
{
  protected static final Logger logger = LoggerFactory.getLogger(JdbcStore.class);
  @NotNull
  private String databaseUrl;
  @NotNull
  private String databaseDriver;
  private Properties connectionProperties;
  protected transient Connection connection = null;

  /*
   * Connection URL used to connect to the specific database.
   */
  @NotNull
  public String getDatabaseUrl()
  {
    return databaseUrl;
  }

  /*
   * Connection URL used to connect to the specific database.
   */
  public void setDatabaseUrl(@NotNull String databaseUrl)
  {
    this.databaseUrl = databaseUrl;
  }

  /*
   * Driver used to connect to the specific database.
   */
  @NotNull
  public String getDatabaseDriver()
  {
    return databaseDriver;
  }

  /*
   * Driver used to connect to the specific database.
   */
  public void setDatabaseDriver(@NotNull String databaseDriver)
  {
    this.databaseDriver = databaseDriver;
  }


  public JdbcStore()
  {
    connectionProperties = new Properties();
  }


  /**
   *  Function to get the connection.
   *  Ignoring "connection" for serialization as it was being used even after
   *  being closed while fetching/displaying the operator properties (refer APEXMALHAR-2351)
   */
  @JsonIgnore
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
    connectionProperties.put("user", userName);
  }

  /**
   * Sets the password.
   *
   * @param password password
   * @deprecated use {@link #setConnectionProperties(String)} instead.
   */
  public void setPassword(String password)
  {
    connectionProperties.put("password", password);
  }

  /**
   * Sets the connection properties on JDBC connection. Connection properties are provided as a string.
   *
   * @param connectionProps Comma separated list of properties. Property key and value are separated by colon.
   *                        eg. user:xyz,password:ijk
   */
  public void setConnectionProperties(String connectionProps)
  {
    String[] properties = Iterables.toArray(Splitter.on(CharMatcher.anyOf(":,")).omitEmptyStrings().trimResults()
        .split(connectionProps), String.class);
    for (int i = 0; i < properties.length; i += 2) {
      if (i + 1 < properties.length) {
        connectionProperties.put(properties[i], properties[i + 1]);
      }
    }
  }

  /**
   * Sets the connection properties on JDBC connection.
   *
   * @param connectionProperties connection properties.
   */
  public void setConnectionProperties(Properties connectionProperties)
  {
    this.connectionProperties = connectionProperties;
  }

  /**
   * Get the connection properties of JDBC connection.
   */
  public Properties getConnectionProperties()
  {
    return connectionProperties;
  }

  /**
   * Create connection with database using JDBC.
   */
  @Override
  public void connect()
  {
    try {
      // This will load the JDBC driver, each DB has its own driver
      Class.forName(databaseDriver).newInstance();
      connection = DriverManager.getConnection(databaseUrl, connectionProperties);

      logger.debug("JDBC connection Success");
    } catch (Throwable t) {
      DTThrowable.rethrow(t);
    }
  }

  /**
   * Close JDBC connection.
   */
  @Override
  public void disconnect()
  {
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException ex) {
        throw new RuntimeException("closing database resource", ex);
      }
    }
  }

  @Override
  @JsonIgnore
  public boolean isConnected()
  {
    try {
      return connection != null ? !connection.isClosed() : false;
    } catch (SQLException e) {
      throw new RuntimeException("is isConnected", e);
    }
  }


}
