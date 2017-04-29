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

package org.apache.apex.malhar.kafka;

import java.util.Properties;

import javax.validation.constraints.NotNull;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import com.datatorrent.api.Context;
import com.datatorrent.api.Operator;

/**
 * This is the base implementation of a Kafka output operator(0.9.0), which writes data to the Kafka message bus.
 *
 * @displayName Abstract Kafka Output
 * @category Messaging
 * @tags output operator
 *
 *
 * @since 3.5.0
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public abstract class AbstractKafkaOutputOperator<K, V> implements Operator
{
  private transient Producer<K, V> producer;
  @NotNull
  private String topic;
  private Properties properties = new Properties();

  @Override
  public void setup(Context.OperatorContext context)
  {
    producer = new KafkaProducer<K, V>(properties);
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

  public Properties getProperties()
  {
    return properties;
  }

  /**
   * Set the Kafka producer properties.
   *
   * @param properties Producer properties
   */
  public void setProperties(Properties properties)
  {
    this.properties.putAll(properties);
  }

  /**
   * Set the Kafka producer property.
   *
   * @param key Producer Property name
   * @param val Producer Property value
   */
  public void setProperty(Object key, Object val)
  {
    properties.put(key, val);
  }

  public String getTopic()
  {
    return topic;
  }

  /**
   * Set the Kafka topic
   * @param  topic  Kafka topic for which the data is sent
   */
  public void setTopic(String topic)
  {
    this.topic = topic;
  }

  protected Producer<K, V> getProducer()
  {
    return producer;
  }
}

