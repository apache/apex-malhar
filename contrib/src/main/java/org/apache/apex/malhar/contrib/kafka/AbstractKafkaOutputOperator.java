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
package org.apache.apex.malhar.contrib.kafka;

import java.util.Properties;
import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.commons.lang3.StringUtils;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

/**
 * This is the base implementation of a Kafka output operator, which writes data to the Kafka message bus.
 * <p>
 * <br>
 * Ports:<br>
 * <b>Input</b>: Can have any number of input ports<br>
 * <b>Output</b>: No output port<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * Class derived from has to implement createKafkaProducerConfig() to setup producer configuration.<br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * Benchmarks:<br>
 * TBD<br>
 * <br>
 * </p>
 *
 * @displayName Abstract Kafka Output
 * @category Messaging
 * @tags output operator
 *
 * @since 0.3.2
 */
public abstract class AbstractKafkaOutputOperator<K, V> implements Operator
{
  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(AbstractKafkaOutputOperator.class);
  private transient kafka.javaapi.producer.Producer<K, V> producer;  // K is key partitioner, V is value type
  @NotNull
  private String topic = "topic1";

  protected int sendCount;

  private String producerProperties = "";

  private Properties configProperties = new Properties();

  public Properties getConfigProperties()
  {
    return configProperties;
  }

  public void setConfigProperties(Properties configProperties)
  {
    this.configProperties = configProperties;
  }


  /**
   * setup producer configuration.
   * @return ProducerConfig
   */
  protected ProducerConfig createKafkaProducerConfig()
  {
    Properties prop = new Properties();
    for (String propString : producerProperties.split(",")) {
      if (!propString.contains("=")) {
        continue;
      }
      String[] keyVal = StringUtils.trim(propString).split("=");
      prop.put(StringUtils.trim(keyVal[0]), StringUtils.trim(keyVal[1]));
    }

    configProperties.putAll(prop);

    return new ProducerConfig(configProperties);
  }

  public Producer<K, V> getProducer()
  {
    return producer;
  }

  public String getTopic()
  {
    return topic;
  }

  public void setTopic(String topic)
  {
    this.topic = topic;
  }

  /**
   * Implement Component Interface.
   *
   * @param context
   */
  @Override
  public void setup(OperatorContext context)
  {
    producer = new Producer<K, V>(createKafkaProducerConfig());
  }

  /**
   * Implement Component Interface.
   */
  @Override
  public void teardown()
  {
    producer.close();
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void beginWindow(long windowId)
  {
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void endWindow()
  {
  }

  /**
   * Return the additional producer properties
   * @return producerProperties
   */
  public String getProducerProperties()
  {
    return producerProperties;
  }

  /**
   * Specify the additional producer properties in comma separated as string in the
   * form of key1=value1,key2=value2,key3=value3,..
   * @param producerProperties Given properties as string
   */
  public void setProducerProperties(String producerProperties)
  {
    this.producerProperties = producerProperties;
  }
}
