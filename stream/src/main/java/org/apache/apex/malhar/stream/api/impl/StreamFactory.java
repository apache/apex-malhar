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
package org.apache.apex.malhar.stream.api.impl;

import org.apache.apex.malhar.contrib.kafka.KafkaSinglePortStringInputOperator;
import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.apex.malhar.kafka.PartitionStrategy;
import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.Option;
import org.apache.hadoop.classification.InterfaceStability;

import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;

import static org.apache.apex.malhar.stream.api.Option.Options.name;

/**
 * A Factory class to build stream from different input sources
 *
 * @since 3.4.0
 */
@InterfaceStability.Evolving
public class StreamFactory
{
  /**
   * Create a stream of string tuples from reading files in hdfs folder line by line
   * @param folderName
   * @param opts
   * @return
   */
  public static ApexStream<String> fromFolder(String folderName, Option... opts)
  {
    LineByLineFileInputOperator fileLineInputOperator = new LineByLineFileInputOperator();
    fileLineInputOperator.setDirectory(folderName);
    ApexStreamImpl<String> newStream = new ApexStreamImpl<>();
    return newStream.addOperator(fileLineInputOperator, null, fileLineInputOperator.output, opts);
  }

  public static ApexStream<String> fromFolder(String folderName)
  {
    return fromFolder(folderName, name("FolderScanner"));
  }

  public static ApexStream<String> fromKafka08(String zookeepers, String topic)
  {
    return fromKafka08(zookeepers, topic, name("Kafka08Input"));
  }

  /**
   * Create a stream of string reading input from kafka 0.8
   * @param zookeepers
   * @param topic
   * @param opts
   * @return
   */
  public static ApexStream<String> fromKafka08(String zookeepers, String topic, Option... opts)
  {
    KafkaSinglePortStringInputOperator kafkaSinglePortStringInputOperator = new KafkaSinglePortStringInputOperator();
    kafkaSinglePortStringInputOperator.getConsumer().setTopic(topic);
    kafkaSinglePortStringInputOperator.getConsumer().setZookeeper(zookeepers);
    ApexStreamImpl<String> newStream = new ApexStreamImpl<>();
    return newStream.addOperator(kafkaSinglePortStringInputOperator, null, kafkaSinglePortStringInputOperator.outputPort);
  }

  /**
   * Create a stream with any input operator
   * @param operator
   * @param outputPort
   * @param opts
   * @param <T>
   * @return
   */
  public static <T> ApexStream<T> fromInput(InputOperator operator, Operator.OutputPort<T> outputPort, Option... opts)
  {
    ApexStreamImpl<T> newStream = new ApexStreamImpl<>();
    return newStream.addOperator(operator, null, outputPort, opts);
  }

  /**
   * Create stream of byte array messages from kafka 0.9
   * @param brokers
   * @param topic
   * @param opts
   * @return
   */
  public static ApexStream<byte[]> fromKafka09(String brokers, String topic, Option... opts)
  {
    KafkaSinglePortInputOperator kafkaInput = new KafkaSinglePortInputOperator();
    kafkaInput.setClusters(brokers);
    kafkaInput.setTopics(topic);
    ApexStreamImpl<String> newStream = new ApexStreamImpl<>();
    return newStream.addOperator(kafkaInput, null, kafkaInput.outputPort, opts);
  }

  /**
   * Create stream of byte array messages from kafka 0.9 with more partition options
   */
  public static ApexStream<byte[]> fromKafka09(String brokers, String topic, PartitionStrategy strategy, int partitionNumber, Option... opts)
  {
    KafkaSinglePortInputOperator kafkaInput = new KafkaSinglePortInputOperator();
    kafkaInput.setClusters(brokers);
    kafkaInput.setTopics(topic);
    kafkaInput.setStrategy(strategy.name());
    kafkaInput.setInitialPartitionCount(partitionNumber);
    ApexStreamImpl<String> newStream = new ApexStreamImpl<>();
    return newStream.addOperator(kafkaInput, null, kafkaInput.outputPort, opts);
  }


}
