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
package org.apache.apex.malhar.contrib.cassandra;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Map;
import java.util.TimeZone;

import org.junit.AfterClass;
import org.junit.BeforeClass;

import org.apache.apex.malhar.lib.db.jdbc.JDBCLookupCacheBackedOperatorTest;

import com.datatorrent.api.Context.OperatorContext;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

/**
 * Tests for Cassandra backed lookup cache.
 */
public class CassandraLookupCacheBackedOperatorTest extends JDBCLookupCacheBackedOperatorTest
{
  private static final String KEYSPACE_NAME = "test";
  private static final String CASSANDRA_DB_URL = "jdbc:cassandra://localhost:9160";
  private static final String CASSANDRA_DB_DRIVER = "org.apache.cassandra.cql.jdbc.CassandraDriver";

  @BeforeClass
  public static void setupDB()
  {
    try {
      // This will load the JDBC driver, each DB has its own driver
      Class.forName(CASSANDRA_DB_DRIVER).newInstance();

      Connection con = DriverManager.getConnection(CASSANDRA_DB_URL);
      Statement stmt = con.createStatement();

      String useSystem = " USE system";
      stmt.executeUpdate(useSystem);
      ResultSet rs = stmt.executeQuery("SELECT * FROM schema_keyspaces");

      boolean foundTest = false;
      while (rs.next()) {
        if (KEYSPACE_NAME.equals(rs.getString(1))) {
          foundTest = true;
        }
      }
      if (!foundTest) {
        String createKeyspace = "CREATE KEYSPACE " + KEYSPACE_NAME +
            " WITH REPLICATION = {'class':'SimpleStrategy', 'replication_factor':3}";
        stmt.executeUpdate(createKeyspace);
      }
      String useKeyspace = "USE " + KEYSPACE_NAME;
      stmt.executeUpdate(useKeyspace);

      String createTable = "CREATE TABLE " + TABLE_NAME + " (col1 int primary key, col2 varchar)";
      stmt.executeUpdate(createTable);

      //populate the database
      for (Map.Entry<Integer, String> entry : mapping.entrySet()) {
        String insert = "INSERT INTO " + TABLE_NAME + " (col1, col2) values (" + entry.getKey() + ", '" + entry.getValue() + "')";
        stmt.executeUpdate(insert);
      }

      //Setup the operator
      lookupCacheBackedOperator.getStore().setDatabaseUrl(CASSANDRA_DB_URL + "/" + KEYSPACE_NAME);
      lookupCacheBackedOperator.getStore().setDatabaseDriver(CASSANDRA_DB_DRIVER);

      Calendar now = Calendar.getInstance(TimeZone.getTimeZone("PST"));
      now.add(Calendar.SECOND, 15);

      SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss z");
      lookupCacheBackedOperator.getCacheManager().setRefreshTime(format.format(now.getTime()));

      lookupCacheBackedOperator.output.setSink(sink);

      OperatorContext context = mockOperatorContext(7);
      lookupCacheBackedOperator.setup(context);
    } catch (Exception ex) {
      logger.error("cassandra setup", ex);
    }

  }

  @AfterClass
  public static void teardown()
  {
    try {
      Connection con = DriverManager.getConnection(CASSANDRA_DB_URL);
      Statement stmt = con.createStatement();

      String dropKeyspace = "DROP KEYSPACE " + KEYSPACE_NAME;
      stmt.executeUpdate(dropKeyspace);
    } catch (Exception ex) {
      logger.error("cassandra teardown", ex);
    }
  }
}
