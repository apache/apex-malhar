/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.kafka;

import com.datatorrent.api.annotation.ShipContainingJars;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Operator;
import javax.validation.constraints.NotNull;
import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka output adapter operator, which produce data into Kafka message bus.<p><br>
 *
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
 *
 * @since 0.3.2
 */
@ShipContainingJars(classes={kafka.javaapi.producer.Producer.class, org.I0Itec.zkclient.ZkClient.class, scala.Function.class})
public abstract class AbstractKafkaOutputOperator<K, V> implements Operator
{
  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(AbstractKafkaOutputOperator.class);
  private transient kafka.javaapi.producer.Producer<K, V> producer;  // K is key partitioner, V is value type
  @NotNull
  private String topic = "topic1";

  protected int sendCount;

  /**
   * Abstract method to setup producer configuration.
   * @return ProducerConfig
   */
  public abstract ProducerConfig createKafkaProducerConfig();

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
}
