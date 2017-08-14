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
package org.apache.apex.malhar.contrib.splunk;

import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.db.Connectable;

import com.splunk.Service;
import com.splunk.ServiceArgs;

/**
 * A {@link Connectable} that uses splunk to connect to stores.
 *
 * @since 1.0.4
 */
public class SplunkStore implements Connectable
{
  protected static final Logger logger = LoggerFactory.getLogger(SplunkStore.class);
  private String userName;
  private String password;
  @NotNull
  protected String host;
  @NotNull
  protected int port;

  protected transient Service service = null;

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
   * Sets the host.
   *
   * @param host host
   */
  public void setHost(@NotNull String host)
  {
    this.host = host;
  }

  /**
   * Sets the port.
   *
   * @param port port
   */
  public void setPort(@NotNull int port)
  {
    this.port = port;
  }

  public Service getService()
  {
    return service;
  }

  /**
   * Create connection with the splunk server.
   */
  @Override
  public void connect()
  {
    try {
      ServiceArgs loginArgs = new ServiceArgs();
      loginArgs.setUsername(userName);
      loginArgs.setPassword(password);
      loginArgs.setHost(host);
      loginArgs.setPort(port);

      service = Service.connect(loginArgs);
    } catch (Exception e) {
      throw new RuntimeException("closing connection", e);
    }
  }

  /**
   * Close connection.
   */
  @Override
  public void disconnect()
  {
    try {
      service.logout();
    } catch (Exception e) {
      throw new RuntimeException("closing connection", e);
    }

  }

  @Override
  public boolean isConnected()
  {
    if (service.getToken() == null) {
      return false;
    } else {
      return true;
    }
  }
}
