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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import org.fusesource.mqtt.client.BlockingConnection;
import org.fusesource.mqtt.client.MQTT;
import org.fusesource.mqtt.client.Message;
import org.fusesource.mqtt.client.QoS;
import org.fusesource.mqtt.client.Topic;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator.ActivationListener;

/**
 * This is the base implementation for and MQTT input operator.&nbsp;
 * A concrete operator should be created from this skeleton implementation.
 * <p></p>
 * @displayName Abstract MQTT Input
 * @category Messaging
 * @tags input operator
 * @since 0.9.3
 */
public abstract class AbstractMqttInputOperator implements InputOperator, ActivationListener<OperatorContext>
{
  private static final Logger LOG = LoggerFactory.getLogger(AbstractMqttInputOperator.class);
  private static final int DEFAULT_BLAST_SIZE = 1000;
  private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
  private int tupleBlast = DEFAULT_BLAST_SIZE;
  private int bufferSize = DEFAULT_BUFFER_SIZE;
  protected Map<String, QoS> topicMap = new HashMap<String, QoS>();
  protected MqttClientConfig mqttClientConfig;
  protected transient MQTT client;
  protected transient ArrayBlockingQueue<Message> holdingBuffer;
  protected transient BlockingConnection connection;
  protected transient Thread thread;

  /**
   * Emits the tuple upon arrival of MQTT message
   *
   * @param message The MQTT message
   */
  public abstract void emitTuple(Message message);

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

  /**
   * Sets the tuple blast
   *
   * @param tupleBlast the number of tuples to blast
   */
  public void setTupleBlast(int tupleBlast)
  {
    this.tupleBlast = tupleBlast;
  }

  /**
   * Adds subscribe topic with the given QoS
   *
   * @param topic the topic
   * @param qos the QoS
   */
  public void addSubscribeTopic(String topic, QoS qos)
  {
    topicMap.put(topic, qos);
  }

  /**
   * Removes the subscribe topic
   *
   * @param topic the topic
   */
  public void removeSubscribeTopic(String topic)
  {
    topicMap.remove(topic);
  }

  @Override
  public void emitTuples()
  {
    int ntuples = tupleBlast;
    if (ntuples > holdingBuffer.size()) {
      ntuples = holdingBuffer.size();
    }
    for (int i = ntuples; i-- > 0;) {
      Message msg = holdingBuffer.poll();
      if (msg == null) {
        break;
      }
      emitTuple(msg);
    }
  }

  @Override
  public void beginWindow(long l)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext t1)
  {
    holdingBuffer = new ArrayBlockingQueue<Message>(bufferSize);
  }

  @Override
  public void teardown()
  {
  }

  private void initializeConnection() throws Exception
  {
    connection = client.blockingConnection();
    connection.connect();
    if (!topicMap.isEmpty()) {
      Topic[] topics = new Topic[topicMap.size()];
      int i = 0;
      for (Map.Entry<String, QoS> entry : topicMap.entrySet()) {
        topics[i++] = new Topic(entry.getKey(), entry.getValue());
      }
      connection.subscribe(topics);
    }
  }

  @Override
  public void activate(OperatorContext context)
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
      }
      if (mqttClientConfig.getWillTopic() != null) {
        client.setWillTopic(mqttClientConfig.getWillTopic());
      }
      initializeConnection();
      thread = new Thread(new Runnable()
      {
        @Override
        public void run()
        {
          while (true) {
            try {
              Message msg = connection.receive();
              holdingBuffer.add(msg);
            } catch (Exception ex) {
              LOG.error("Trouble receiving", ex);
            }
          }
        }

      });
      thread.start();
    } catch (Exception ex) {
      LOG.error("Caught exception during activation: ", ex);
      throw new RuntimeException(ex);
    }

  }

  @Override
  public void deactivate()
  {
    try {
      thread.interrupt();
      thread.join();
    } catch (InterruptedException ex) {
      LOG.error("interrupted");
    } finally {
      try {
        connection.disconnect();
      } catch (Exception ex) {
        LOG.error("Caught exception during disconnect", ex);
      }
    }
  }

}
