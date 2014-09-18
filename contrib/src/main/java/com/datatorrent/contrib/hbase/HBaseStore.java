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
package com.datatorrent.contrib.hbase;

import com.datatorrent.lib.db.Connectable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

import java.io.IOException;
/**
 * A {@link Connectable} that uses hbase to connect to stores.
 * <p>
 * @displayName: HBase Store
 * @category: store
 * @tag: store
 * @since 1.0.2
 */
public class HBaseStore implements Connectable {

  private String zookeeperQuorum;
  private int zookeeperClientPort;
  protected String tableName;

  protected transient HTable table;

  /**
   * Get the zookeeper quorum location.
   * 
   * @return The zookeeper quorum location
   */
  public String getZookeeperQuorum() {
    return zookeeperQuorum;
  }

  /**
   * Set the zookeeper quorum location.
   * 
   * @param zookeeperQuorum
   *            The zookeeper quorum location
   */
  public void setZookeeperQuorum(String zookeeperQuorum) {
    this.zookeeperQuorum = zookeeperQuorum;
  }

  /**
   * Get the zookeeper client port.
   * 
   * @return The zookeeper client port
   */
  public int getZookeeperClientPort() {
    return zookeeperClientPort;
  }

  /**
   * Set the zookeeper client port.
   * 
   * @param zookeeperClientPort
   *            The zookeeper client port
   */
  public void setZookeeperClientPort(int zookeeperClientPort) {
    this.zookeeperClientPort = zookeeperClientPort;
  }

  /**
   * Get the HBase table name.
   * 
   * @return The HBase table name
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * Set the HBase table name.
   * 
   * @param tableName
   *            The HBase table name
   */
  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  /**
   * Get the HBase table .
   * 
   * @return The HBase table
   */
  public HTable getTable() {
    return table;
  }

  /**
   * Set the HBase table.
   * 
   * @param table
   *            The HBase table
   */
  public void setTable(HTable table) {
    this.table = table;
  }

  /**
   * Get the configuration.
   * 
   * @return The configuration
   */
  public Configuration getConfiguration() {
    return configuration;
  }

  /**
   * Set the configuration.
   * 
   * @param configuration
   *            The configuration
   */
  public void setConfiguration(Configuration configuration) {
    this.configuration = configuration;
  }

  protected transient Configuration configuration;

  @Override
  public void connect() throws IOException {
    configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
    configuration.set("hbase.zookeeper.property.clientPort", "" 	+ zookeeperClientPort);
    table = new HTable(configuration, tableName);
    table.setAutoFlushTo(false);

  }

  @Override
  public void disconnect() throws IOException {
    // not applicable for hbase

  }

  @Override
  public boolean connected() {
    // not applicable to hbase
    return false;
  }

}
