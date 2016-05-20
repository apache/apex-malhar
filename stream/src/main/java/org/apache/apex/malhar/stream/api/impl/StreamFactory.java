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

import org.apache.apex.malhar.lib.fs.LineByLineFileInputOperator;
import org.apache.apex.malhar.stream.api.ApexStream;

import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;

/**
 * A Factory class to build from different kind of input source
 *
 * @since 3.4.0
 */
public class StreamFactory
{
  public static ApexStream<String> fromFolder(String inputOperatorName, String folderName)
  {
    LineByLineFileInputOperator fileLineInputOperator = new LineByLineFileInputOperator();
    fileLineInputOperator.setDirectory(folderName);
    ApexStreamImpl<String> newStream = new ApexStreamImpl<>();
    return newStream.addOperator(inputOperatorName, fileLineInputOperator, null, fileLineInputOperator.output);
  }

  public static ApexStream<String> fromFolder(String folderName)
  {
    return fromFolder("FolderScanner", folderName);
  }

  public static ApexStream<String> fromKafka08(String zookeepers, String topic)
  {
    return fromKafka08("Kafka08Input", zookeepers, topic);
  }

  public static ApexStream<String> fromKafka08(String inputName, String zookeepers, String topic)
  {
    KafkaSinglePortStringInputOperator kafkaSinglePortStringInputOperator = new KafkaSinglePortStringInputOperator();
    kafkaSinglePortStringInputOperator.getConsumer().setTopic(topic);
    kafkaSinglePortStringInputOperator.getConsumer().setZookeeper(zookeepers);
    ApexStreamImpl<String> newStream = new ApexStreamImpl<>();
    return newStream.addOperator(kafkaSinglePortStringInputOperator, null, kafkaSinglePortStringInputOperator.outputPort);
  }

  public static <T> ApexStream<T> fromInput(String inputOperatorName, InputOperator operator, Operator.OutputPort<T> outputPort)
  {
    ApexStreamImpl<T> newStream = new ApexStreamImpl<>();
    return newStream.addOperator(inputOperatorName, operator, null, outputPort);
  }

  public static <T> ApexStream<T> fromInput(InputOperator operator, Operator.OutputPort<T> outputPort)
  {
    return fromInput(operator.toString(), operator, outputPort);
  }

  public static ApexStream<String> fromKafka09(String name, String brokers, String topic)
  {
    throw new UnsupportedOperationException();
  }

  public static ApexStream<String> fromKafka09(String brokers, String topic)
  {
    return fromKafka09("KafkaInput", brokers, topic);
  }

}
