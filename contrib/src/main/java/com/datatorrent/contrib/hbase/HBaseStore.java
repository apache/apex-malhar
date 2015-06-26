/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.hbase;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public static final String USER_NAME_SPECIFIER = "%USER_NAME%";
  
  private static final Logger logger = LoggerFactory.getLogger(HBaseStore.class);
  
  private String zookeeperQuorum;
  private int zookeeperClientPort;
  protected String tableName;
  
  protected String principal;
  protected String keytabPath;
  // Default interval 30 min
  protected long reloginCheckInterval = 30 * 60 * 1000;
  protected transient Thread loginRenewer;
  private volatile transient boolean doRelogin;

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
  public String getKeytabPath()
  {
    return keytabPath;
  }

  /**
   * Set the Kerberos keytab path.
   *
   * @param keytabPath
   *            The Kerberos keytab path
   */
  public void setKeytabPath(String keytabPath)
  {
    this.keytabPath = keytabPath;
  }

  /**
   * Get the interval to check for relogin.
   *
   * @return The interval to check for relogin
   */
  public long getReloginCheckInterval()
  {
    return reloginCheckInterval;
  }

  /**
   * Set the interval to check for relogin.
   *
   * @param reloginCheckInterval
   *            The interval to check for relogin
   */
  public void setReloginCheckInterval(long reloginCheckInterval)
  {
    this.reloginCheckInterval = reloginCheckInterval;
  }

  /**
   * Get the HBase table .
   * 
   * @return The HBase table
   * @omitFromUI
   */
  public HTable getTable() {
    return table;
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
    if ((principal != null) && (keytabPath != null)) {
      String lprincipal = evaluateProperty(principal);
      String lkeytabPath = evaluateProperty(keytabPath);
      UserGroupInformation.loginUserFromKeytab(lprincipal, lkeytabPath);
      doRelogin = true;
      loginRenewer = new Thread(new Runnable()
      {
        @Override
        public void run()
        {
          logger.debug("Renewer starting");
          try {
            while (doRelogin) {
              Thread.sleep(reloginCheckInterval);
              try {
                UserGroupInformation.getLoginUser().checkTGTAndReloginFromKeytab();
              } catch (IOException e) {
                logger.error("Error trying to relogin from keytab", e);
              }
            }
          } catch (InterruptedException e) {
            if (doRelogin) {
              logger.warn("Renewer interrupted... stopping");
            }
          }
          logger.debug("Renewer ending");
        }
      });
      loginRenewer.start();
    }
    configuration = HBaseConfiguration.create();
    // The default configuration is loaded from resources in classpath, the following parameters can be optionally set
    // to override defaults
    if (zookeeperQuorum != null) {
      configuration.set("hbase.zookeeper.quorum", zookeeperQuorum);
    }
    if (zookeeperClientPort != 0) {
      configuration.set("hbase.zookeeper.property.clientPort", "" + zookeeperClientPort);
    }
    table = new HTable(configuration, tableName);
    table.setAutoFlushTo(false);

  }
  
  private String evaluateProperty(String property) throws IOException
  {
    if (property.contains(USER_NAME_SPECIFIER)) {
     property = property.replaceAll(USER_NAME_SPECIFIER, UserGroupInformation.getLoginUser().getShortUserName()); 
    }
    return property;
  }

  @Override
  public void disconnect() throws IOException {
    if (loginRenewer != null) {
      doRelogin = false;
      loginRenewer.interrupt();
      try {
        loginRenewer.join();
      } catch (InterruptedException e) {
        logger.warn("Unsuccessful waiting for renewer to finish. Proceeding to shutdown", e);
      }
    }
  }

  @Override
  public boolean isConnected() {
    // not applicable to hbase
    return false;
  }

}
