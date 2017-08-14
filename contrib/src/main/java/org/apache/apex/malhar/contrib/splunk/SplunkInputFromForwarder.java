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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 *
 * An abstract class which receives input from a splunk forwarder and writes the lines to kafka.
 * A kafka input operator can be used to read these lines in an Apex application.
 *
 * @param <T> the type of data to be stored into kafka
 * @since 1.0.4
 */
public abstract class SplunkInputFromForwarder<T>
{
  private final int DEFAULT_PORT = 6789;
  private int port;
  protected Producer<String, T> producer;
  private String topic;
  private Properties configProperties = new Properties();
  protected ServerSocket serverSocket;
  protected Socket connectionSocket;

  public SplunkInputFromForwarder()
  {
    port = DEFAULT_PORT;
  }

  public int getPort()
  {
    return port;
  }

  public void setPort(int port)
  {
    this.port = port;
  }

  public String getTopic()
  {
    return topic;
  }

  public void setTopic(String topic)
  {
    this.topic = topic;
  }

  public Properties getConfigProperties()
  {
    return configProperties;
  }

  public void setConfigProperties(Properties configProperties)
  {
    this.configProperties = configProperties;
  }

  public abstract T getMessage(String line);

  public void writeToKafka(String line)
  {
    T message = null;
    if (line != null) {
      message = getMessage(line);
    }
    if (message != null) {
      producer.send(new KeyedMessage<String, T>(getTopic(), message));
    }
  }

  public void startServer() throws IOException
  {
    ProducerConfig producerConfig = new ProducerConfig(configProperties);
    producer = new Producer<String, T>(producerConfig);
    serverSocket = new ServerSocket(port);
  }

  public void process() throws IOException
  {
    connectionSocket = serverSocket.accept();
    String line;
    BufferedReader reader = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
    while (true) {
      line = reader.readLine();
      writeToKafka(line);
    }
  }

  public void stopServer() throws IOException
  {
    serverSocket.close();
    connectionSocket.close();
    producer.close();
  }
}
