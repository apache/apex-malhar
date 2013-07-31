/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for all JDBC input output operators.
 * This handles JDBC connection and column mapping for output operators.
 *
 * @since 0.3.2
 * @author Locknath Shil <locknath@datatorrent.com>
 */
public abstract class JDBCOperatorBase
{
  private static final Logger logger = LoggerFactory.getLogger(JDBCOperatorBase.class);

  @NotNull
  private String dbUrl;
  @NotNull
  private String dbDriver;
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
   * Create connection with database using JDBC.
   */
  public void setupJDBCConnection()
  {
    try {
      // This will load the JDBC driver, each DB has its own driver
      Class.forName(dbDriver).newInstance();
      connection = DriverManager.getConnection(dbUrl);

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
  public void closeJDBCConnection()
  {
    try {
      connection.close();
    }
    catch (SQLException ex) {
      throw new RuntimeException("Error while closing database resource", ex);
    }
  }
}
