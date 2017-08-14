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
package org.apache.apex.malhar.contrib.hbase;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

/**
 * Implements the base class for the HBase operators. <br>
 * <p>
 * <br>
 * The HBase operators extend this class. The base class contains the HBase properties and also sets up
 * the configuration to be used by the HBase operators.<br>
 *
 * <br>
 * @displayName HBase Operator Base
 * @category Output
 * @tags hbase, base, properties
 * @since 0.3.2
 */
public class HBaseOperatorBase
{
  private String zookeeperQuorum;
  private int zookeeperClientPort;
  protected String tableName;

  protected transient HTable table;
  protected transient Configuration configuration;

  /**
   * Get the zookeeper quorum location.
   * @return The zookeeper quorum location
   */
  public String getZookeeperQuorum()
  {
    return zookeeperQuorum;
  }

  /**
   * Set the zookeeper quorum location.
   * @param zookeeperQuorum The zookeeper quorum location
   */
  public void setZookeeperQuorum(String zookeeperQuorum)
  {
    this.zookeeperQuorum = zookeeperQuorum;
  }

  /**
   * Get the zookeeper client port.
   * @return The zookeeper client port
   */
  public int getZookeeperClientPort()
  {
    return zookeeperClientPort;
  }

  /**
   * Set the zookeeper client port.
   * @param zookeeperClientPort The zookeeper client port
   */
  public void setZookeeperClientPort(int zookeeperClientPort)
  {
    this.zookeeperClientPort = zookeeperClientPort;
  }

  /**
   * Get the HBase table name.
   * @return The HBase table name
   */
  public String getTableName()
  {
    return tableName;
  }

  /**
   * Set the HBase table name.
   * @param tableName The HBase table name
   */
  public void setTableName(String tableName)
  {
    this.tableName = tableName;
  }

  /**
   * Get the configuration.
   * @return The configuration
   */
  public Configuration getConfiguration()
  {
    return configuration;
  }

  /**
   * Setup the configuration.
   * Setup the configuration using the different properties set in the class. This method
   * should be called before using the configuration or table.
   * @throws IOException
   */
  protected void setupConfiguration() throws IOException
  {
    configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
    configuration.set("hbase.zookeeper.property.clientPort", "" + zookeeperClientPort);
    table = new HTable(configuration, tableName);
  }

  /**
   * Get the HBase table.
   * If setupConfiguration was called and was successful this method returns the table
   * that was setup by the method. Otherwise null is returned.
   * @return The HBase table if configuration setup was successful, null otherwise
   * @throws IOException
   */
  protected HTable getTable() throws IOException
  {
    return table;
  }

}
