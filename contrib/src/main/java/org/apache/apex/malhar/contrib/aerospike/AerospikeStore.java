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
package org.apache.apex.malhar.contrib.aerospike;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.db.Connectable;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.AerospikeException;
import com.datatorrent.netlet.util.DTThrowable;

/**
 * A {@link Connectable} that uses aerospike to connect to stores and implements Connectable interface.
 *
 * @displayName Aerospike Store
 * @category Output
 * @tags store
 * @since 1.0.4
 */
public class AerospikeStore implements Connectable
{
  protected static final Logger logger = LoggerFactory.getLogger(AerospikeStore.class);
  private String userName;
  private String password;
  @NotNull
  private String node;
  private int port;
  protected transient AerospikeClient client = null;

  /**
   * Sets the user name.
   *
   * @param userName user name.
   */
  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  /**
   * Sets the password.
   *
   * @param password password
   */
  public void setPassword(String password)
  {
    this.password = password;
  }

  /**
   * Get the node.
   *
   * @return The node
   */
  @NotNull
  public String getNode()
  {
    return node;
  }

  /**
   * Sets the node.
   *
   * @param node node
   */
  public void setNode(@NotNull String node)
  {
    this.node = node;
  }

  /**
   * Get the client.
   *
   * @return The client
   */
  public AerospikeClient getClient()
  {
    return client;
  }

  /**
   * Sets the port.
   *
   * @param port port
   */
  public void setPort(int port)
  {
    this.port = port;
  }

  /**
   * Create connection with database.
   */
  @Override
  public void connect()
  {
    try {
      client = new AerospikeClient(node, port);
      logger.debug("Aerospike connection Success");
    } catch (AerospikeException ex) {
      throw new RuntimeException("closing database resource", ex);
    } catch (Throwable t) {
      DTThrowable.rethrow(t);
    }
  }

  /**
   * Close connection.
   */
  @Override
  public void disconnect()
  {
    client.close();
  }

  @Override
  public boolean isConnected()
  {
    return !client.isConnected();
  }
}
