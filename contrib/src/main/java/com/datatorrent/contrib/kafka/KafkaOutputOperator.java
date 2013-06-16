/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.kafka;

import com.malhartech.api.annotation.ShipContainingJars;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.Operator;
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
 * @author Locknath Shil <locknath@malhar-inc.com>
 *
 */
@ShipContainingJars(classes={kafka.javaapi.producer.Producer.class})
public abstract class KafkaOutputOperator<K, V> implements Operator
{
  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(KafkaOutputOperator.class);
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
