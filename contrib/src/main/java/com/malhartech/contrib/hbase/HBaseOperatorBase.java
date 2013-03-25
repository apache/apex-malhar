/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.hbase;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;

/**
 *
 * @author Pramod Immaneni <pramod@malhar-inc.com>
 */
public class HBaseOperatorBase
{
  private String zookeeperQuorum;
  private int zookeeperClientPort;
  protected String tableName;

  protected transient HTable table;
  protected transient Configuration configuration;

  public String getZookeeperQuorum()
  {
    return zookeeperQuorum;
  }

  public void setZookeeperQuorum(String zookeeperQuorum)
  {
    this.zookeeperQuorum = zookeeperQuorum;
  }

  public int getZookeeperClientPort()
  {
    return zookeeperClientPort;
  }

  public void setZookeeperClientPort(int zookeeperClientPort)
  {
    this.zookeeperClientPort = zookeeperClientPort;
  }

  public String getTableName()
  {
    return tableName;
  }

  public void setTableName(String tableName)
  {
    this.tableName = tableName;
  }

  public Configuration getConfiguration() {
    return configuration;
  }

  protected void setupConfiguration() throws IOException {
    configuration = HBaseConfiguration.create();
    configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
    configuration.set("hbase.zookeeper.property.clientPort", "" + zookeeperClientPort);
    table = new HTable(configuration, tableName);
  }

  protected HTable getTable() throws IOException {
    return table;
  }

}
