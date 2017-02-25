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
package org.apache.apex.examples.distributeddistinct;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;

public class StatefulApplicationTest
{

  @BeforeClass
  public static void setup()
  {
    try {
      Class.forName(StatefulUniqueCountTest.INMEM_DB_DRIVER).newInstance();
      Connection con = DriverManager.getConnection(StatefulUniqueCountTest.INMEM_DB_URL, new Properties());
      Statement stmt = con.createStatement();
      stmt.execute("CREATE TABLE IF NOT EXISTS " + StatefulUniqueCountTest.TABLE_NAME + " (col1 INTEGER, col2 INTEGER, col3 BIGINT)");
    } catch (InstantiationException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void testApplication() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.set("dt.operator.StatefulUniqueCounter.prop.tableName", "Test_Lookup_Cache");
    conf.set("dt.operator.StatefulUniqueCounter.prop.store.dbUrl", "jdbc:hsqldb:mem:test;sql.syntax_mys=true");
    conf.set("dt.operator.StatefulUniqueCounter.prop.store.dbDriver", "org.hsqldb.jdbcDriver");

    lma.prepareDAG(new StatefulApplication(), conf);
    lma.cloneDAG();
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);
    lc.runAsync();

    long now = System.currentTimeMillis();
    while (System.currentTimeMillis() - now < 15000) {
      Thread.sleep(1000);
    }

    lc.shutdown();
  }
}
