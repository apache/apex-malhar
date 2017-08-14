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
package org.apache.apex.malhar.contrib.accumulo;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.apex.malhar.lib.db.Connectable;

import com.datatorrent.netlet.util.DTThrowable;

/**
 * A {@link Connectable} for accumulo that implements Connectable interface.
 * <p>
 * @displayName Accumulo Store
 * @category Output
 * @tags key value, accumulo
 * @param <T>
 * @since 1.0.4
 */
public class AccumuloStore implements Connectable
{
  private static final transient Logger logger = LoggerFactory.getLogger(AccumuloStore.class);
  private String zookeeperHost;
  private String instanceName;
  private String userName;
  private String password;
  protected String tableName;

  protected transient Connector connector;
  protected transient BatchWriter batchwriter;

  private long memoryLimit;
  private int numThreads;
  private static final long DEFAULT_MEMORY = 2147483648L;
  private static final int DEFAULT_THREADS = 1;

  public AccumuloStore()
  {
    memoryLimit = DEFAULT_MEMORY;
    numThreads = DEFAULT_THREADS;
  }

  /**
   * getter for Connector
   *
   * @return Connector
   */
  public Connector getConnector()
  {
    return connector;
  }

  /**
   * getter for TableName
   *
   * @return TableName
   */
  public String getTableName()
  {
    return tableName;
  }

  /**
   * setter for TableName
   *
   * @param tableName
   */
  public void setTableName(String tableName)
  {
    this.tableName = tableName;
  }

  /**
   * getter for zookeeper host address
   *
   * @return ZookeeperHost
   */
  public String getZookeeperHost()
  {
    return zookeeperHost;
  }

  /**
   * setter for zookeeper host address
   *
   * @param zookeeperHost
   */
  public void setZookeeperHost(String zookeeperHost)
  {
    this.zookeeperHost = zookeeperHost;
  }

  /**
   * getter for instanceName
   *
   * @return instanceName
   */
  public String getInstanceName()
  {
    return instanceName;
  }

  /**
   * setter for instanceName
   *
   * @param instanceName
   */
  public void setInstanceName(String instanceName)
  {
    this.instanceName = instanceName;
  }

  /**
   * setter for userName
   *
   * @param userName
   */
  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  /**
   * setter for password
   *
   * @param password
   */
  public void setPassword(String password)
  {
    this.password = password;
  }

  /**
   * setter for memory limit
   *
   * @param memoryLimit
   */
  public void setMemoryLimit(long memoryLimit)
  {
    this.memoryLimit = memoryLimit;
  }

  /**
   * setter for number of writer threads
   *
   * @param numThreads
   */
  public void setNumThreads(int numThreads)
  {
    this.numThreads = numThreads;
  }

  /**
   * getter for BatchWriter
   *
   * @return BatchWriter
   */
  public BatchWriter getBatchwriter()
  {
    return batchwriter;
  }

  @Override
  public void connect() throws IOException
  {
    Instance instance = null;
    instance = new ZooKeeperInstance(instanceName, zookeeperHost);
    try {
      PasswordToken t = new PasswordToken(password.getBytes());
      connector = instance.getConnector(userName, t);
    } catch (AccumuloException e) {
      logger.error("error connecting to accumulo", e);
      DTThrowable.rethrow(e);
    } catch (AccumuloSecurityException e) {
      logger.error("error connecting to accumulo", e);
      DTThrowable.rethrow(e);
    }
    BatchWriterConfig config = new BatchWriterConfig();
    config.setMaxMemory(memoryLimit);
    config.setMaxWriteThreads(numThreads);
    try {
      batchwriter = connector.createBatchWriter(tableName, config);
    } catch (TableNotFoundException e) {
      logger.error("table not found", e);
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public void disconnect() throws IOException
  {
    try {
      batchwriter.close();
    } catch (MutationsRejectedException e) {
      logger.error("mutation rejected during batchwriter close", e);
      DTThrowable.rethrow(e);
    }
  }

  @Override
  public boolean isConnected()
  {
    // Not applicable for accumulo
    return false;
  }

}
