/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.contrib.kafka;

import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.DefaultInputPort;
import kafka.javaapi.producer.ProducerData;

/**
 * Kafka output adapter operator with only one input port, which produce data into Kafka message bus.<p><br>
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: Have only one input port<br>
 * <b>Output</b>: No output port<br>
 * <br>
 * Properties:<br>
 * None<br>
 * <br>
 * Compile time checks:<br>
 * Class derived from this has to implement the abstract method createKafkaProducerConfig() to setup producer configuration.<br>
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
public abstract class KafkaSinglePortOutputOperator<K, V> extends KafkaOutputOperator
{
  /**
   * The single input port.
   */
  @InputPortFieldAnnotation(name = "KafkaInputPort")
  public final transient DefaultInputPort<V> inputPort = new DefaultInputPort<V>(this)
  {
    @Override
    public void process(V tuple)
    {
      // Send out single data
      getProducer().send(new ProducerData<K, V>(getTopic(), tuple));
      sendCount++;

      // TBD: Kafka also has an api to send out bunch of data in a list.
      // which is not yet supported here.

      //logger.debug("process message {}", tuple.toString());
    }
  };
}
