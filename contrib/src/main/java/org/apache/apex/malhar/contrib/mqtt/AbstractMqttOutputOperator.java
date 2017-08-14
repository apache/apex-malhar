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

import javax.validation.constraints.NotNull;
import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is the base implementation of an MQTT output operator.&nbsp;
 * A concrete operator should be created from this skeleton implementation.
 * <p></p>
 * @displayName Abstract MQTT Output
 * @category Messaging
 * @tags output operator
 * @since 0.9.3
 */
public class AbstractMqttOutputOperator extends BaseOperator
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractMqttOutputOperator.class);
  @NotNull
  protected MqttClientConfig mqttClientConfig;
  protected transient MQTT client;
  protected transient BlockingConnection connection;

  @Override
  public void setup(OperatorContext context)
  {
    try {
      client = new MQTT();
      if (mqttClientConfig.getClientId() != null) {
        client.setClientId(mqttClientConfig.getClientId());
      }
      client.setCleanSession(mqttClientConfig.isCleanSession());
      client.setConnectAttemptsMax(mqttClientConfig.getConnectAttemptsMax());
      client.setHost(mqttClientConfig.getHost(), mqttClientConfig.getPort());
      client.setKeepAlive(mqttClientConfig.getKeepAliveInterval());
      if (mqttClientConfig.getPassword() != null) {
        client.setPassword(mqttClientConfig.getPassword());
      }
      if (mqttClientConfig.getUserName() != null) {
        client.setUserName(mqttClientConfig.getUserName());
      }
      if (mqttClientConfig.getWillMessage() != null) {
        client.setWillMessage(mqttClientConfig.getWillMessage());
        client.setWillQos(mqttClientConfig.getWillQos());
        client.setWillRetain(mqttClientConfig.isWillRetain());
        client.setWillTopic(mqttClientConfig.getWillTopic());
      }
      connection = client.blockingConnection();
      connection.connect();
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  @Override
  public void teardown()
  {
    try {
      connection.disconnect();
    } catch (Exception ex) {
      //ignore
    }
  }

  /**
   * Gets the MQTT config object
   *
   * @return the config object
   */
  public MqttClientConfig getMqttClientConfig()
  {
    return mqttClientConfig;
  }

  /**
   * Sets the MQTT config object
   *
   * @param mqttClientConfig the config object
   */
  public void setMqttClientConfig(MqttClientConfig mqttClientConfig)
  {
    this.mqttClientConfig = mqttClientConfig;
  }

}
