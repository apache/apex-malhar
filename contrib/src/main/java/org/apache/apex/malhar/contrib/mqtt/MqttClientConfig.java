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
package org.apache.apex.malhar.contrib.mqtt;

import org.fusesource.mqtt.client.QoS;

/**
 * The config class for MQTT
 * Please refer to MQTT documentation of what each field means
 *
 * @since 0.9.3
 */
public class MqttClientConfig
{
  private String clientId;
  private short keepAliveInterval;
  private String willTopic;
  private String willMessage;
  private QoS willQos = QoS.AT_MOST_ONCE;
  private boolean willRetain;
  private String userName;
  private String password;
  private boolean cleanSession;
  private int connectionTimeout = 500;
  private int connectAttemptsMax = 1;
  private String host = "localhost";
  private int port = 1883;

  /**
   * Gets the MQTT client ID
   *
   * @return the client ID
   */
  public String getClientId()
  {
    return clientId;
  }

  /**
   * Sets the Client ID
   *
   * @param clientId the client ID
   */
  public void setClientId(String clientId)
  {
    this.clientId = clientId;
  }

  /**
   * Gets the keep alive interval
   *
   * @return the keep alive interval
   */
  public short getKeepAliveInterval()
  {
    return keepAliveInterval;
  }

  /**
   * Sets the keep alive interval
   *
   * @param keepAliveInterval the keep alive interval
   */
  public void setKeepAliveInterval(short keepAliveInterval)
  {
    this.keepAliveInterval = keepAliveInterval;
  }

  /**
   * Gets the will topic
   *
   * @return the will topic
   */
  public String getWillTopic()
  {
    return willTopic;
  }

  /**
   * Sets the will topic
   *
   * @param willTopic the will topic
   */
  public void setWillTopic(String willTopic)
  {
    this.willTopic = willTopic;
  }

  /**
   * Gets the will message
   *
   * @return the will message
   */
  public String getWillMessage()
  {
    return willMessage;
  }

  /**
   * Sets the will message
   *
   * @param willMessage the will message
   */
  public void setWillMessage(String willMessage)
  {
    this.willMessage = willMessage;
  }

  /**
   * Gets the will QoS
   *
   * @return the will qos
   */
  public QoS getWillQos()
  {
    return willQos;
  }

  /**
   * Sets the will qos
   *
   * @param willQos the will qos
   */
  public void setWillQos(QoS willQos)
  {
    this.willQos = willQos;
  }

  /**
   * Gets whether will is retain
   *
   * @return whether will is retain
   */
  public boolean isWillRetain()
  {
    return willRetain;
  }

  /**
   * Sets whether will is retain
   *
   * @param willRetain whether will is retain
   */
  public void setWillRetain(boolean willRetain)
  {
    this.willRetain = willRetain;
  }

  /**
   * Gets the user name
   *
   * @return the user name
   */
  public String getUserName()
  {
    return userName;
  }

  /**
   * Sets the user name
   *
   * @param userName the user name
   */
  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  /**
   * Gets the password
   *
   * @return the password
   */
  public String getPassword()
  {
    return password;
  }

  /**
   * Sets the password
   *
   * @param password the password
   */
  public void setPassword(String password)
  {
    this.password = password;
  }

  /**
   * Gets whether it's a clean session
   *
   * @return whether it's a clean session
   */
  public boolean isCleanSession()
  {
    return cleanSession;
  }

  /**
   * Sets whether it's a clean session
   *
   * @param cleanSession whether it's a clean session
   */
  public void setCleanSession(boolean cleanSession)
  {
    this.cleanSession = cleanSession;
  }

  /**
   * Gets the connection timeout
   *
   * @return the connection timeout
   */
  public int getConnectionTimeout()
  {
    return connectionTimeout;
  }

  /**
   * Sets the connection timeout
   *
   * @param connectionTimeout connection timeout
   */
  public void setConnectionTimeout(int connectionTimeout)
  {
    this.connectionTimeout = connectionTimeout;
  }

  /**
   * Gets the max attempts of connect
   *
   * @return the max attempts of connect
   */
  public int getConnectAttemptsMax()
  {
    return connectAttemptsMax;
  }

  /**
   * Sets the max attempts of connect
   *
   * @param connectAttemptsMax the max attempts of connect
   */
  public void setConnectAttemptsMax(int connectAttemptsMax)
  {
    this.connectAttemptsMax = connectAttemptsMax;
  }

  /**
   * Gets the host
   *
   * @return the host
   */
  public String getHost()
  {
    return host;
  }

  /**
   * Sets the host
   *
   * @param host the host
   */
  public void setHost(String host)
  {
    this.host = host;
  }

  /**
   * Gets the port
   *
   * @return the port
   */
  public int getPort()
  {
    return port;
  }

  /**
   * Sets the port
   *
   * @param port the port
   */
  public void setPort(int port)
  {
    this.port = port;
  }

}
