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

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.security.UserGroupInformation;

import com.datatorrent.lib.db.Connectable;
/**
 * A {@link Connectable} that uses HBase to connect to stores and implements Connectable interface. 
 * <p>
 * @displayName HBase Store
 * @category Store
 * @tags store
 * @since 1.0.2
 */
public class HBaseStore implements Connectable {

  private String zookeeperQuorum;
  private int zookeeperClientPort;
  protected String tableName;
  
  protected String principal;
  protected String keytab;

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
   * Get the Kerberos principal.
   *
   * @return The Kerberos principal
   */
  public String getPrincipal()
  {
    return principal;
  }

  /**
   * Set the Kerberos principal.
   *
   * @param principal
   *            The Kerberos principal
   */
  public void setPrincipal(String principal)
  {
    this.principal = principal;
  }

  /**
   * Get the Kerberos keytab path
   *
   * @return The Kerberos keytab path
   */
  public String getKeytab()
  {
    return keytab;
  }

  /**
   * Set the Kerberos keytab path.
   *
   * @param keytab
   *            The Kerberos keytab path
   */
  public void setKeytab(String keytab)
  {
    this.keytab = keytab;
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
    if ((principal != null) && (keytab != null)) {
      UserGroupInformation.loginUserFromKeytab(principal, keytab);
    }
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
  public boolean isConnected() {
    // not applicable to hbase
    return false;
  }

}
