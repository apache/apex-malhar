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
package org.apache.apex.malhar.sql.table;

import java.util.Map;
import java.util.Properties;

import org.apache.apex.malhar.kafka.KafkaSinglePortInputOperator;
import org.apache.apex.malhar.kafka.KafkaSinglePortOutputOperator;
import org.apache.apex.malhar.sql.operators.OperatorUtils;
import org.apache.apex.malhar.sql.planner.RelInfo;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;

/**
 * This is an implementation of {@link Endpoint} which defined how data should be read/written from kafka messaging system
 *
 * @since 3.6.0
 */
@InterfaceStability.Evolving
public class KafkaEndpoint implements Endpoint
{
  public static final String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
  public static final String VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
  public static final String KEY_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";
  public static final String VALUE_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";


  public static final String KAFKA_SERVERS = "servers";
  public static final String KAFKA_TOPICS = "topics";

  private MessageFormat messageFormat;

  private Map<String, Object> operands;

  public KafkaEndpoint()
  {
  }

  public KafkaEndpoint(String kafkaServers, String topics, MessageFormat messageFormat)
  {
    this.messageFormat = messageFormat;
    this.operands = ImmutableMap.<String, Object>of(KAFKA_SERVERS, kafkaServers, KAFKA_TOPICS, topics);
  }

  @Override
  public EndpointType getTargetType()
  {
    return EndpointType.KAFKA;
  }

  @Override
  public void setEndpointOperands(Map<String, Object> operands)
  {
    this.operands = operands;
  }

  @Override
  public void setMessageFormat(MessageFormat messageFormat)
  {
    this.messageFormat = messageFormat;
  }

  @Override
  public RelInfo populateInputDAG(DAG dag, JavaTypeFactory typeFactory)
  {
    KafkaSinglePortInputOperator kafkaInput = dag.addOperator(OperatorUtils.getUniqueOperatorName("KafkaInput"),
        KafkaSinglePortInputOperator.class);
    kafkaInput.setTopics((String)operands.get(KAFKA_TOPICS));
    kafkaInput.setInitialOffset("EARLIEST");

    Properties props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, operands.get(KAFKA_SERVERS));
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, KEY_DESERIALIZER);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, VALUE_DESERIALIZER);
    kafkaInput.setConsumerProps(props);

    kafkaInput.setClusters((String)operands.get(KAFKA_SERVERS));

    RelInfo spec = messageFormat.populateInputDAG(dag, typeFactory);
    dag.addStream(OperatorUtils.getUniqueStreamName("Kafka", "Parser"), kafkaInput.outputPort,
        spec.getInputPorts().get(0));
    return new RelInfo("Input", Lists.<Operator.InputPort>newArrayList(), spec.getOperator(), spec.getOutPort(),
        messageFormat.getRowType(typeFactory));
  }

  @Override
  public RelInfo populateOutputDAG(DAG dag, JavaTypeFactory typeFactory)
  {
    RelInfo spec = messageFormat.populateOutputDAG(dag, typeFactory);

    KafkaSinglePortOutputOperator kafkaOutput = dag.addOperator(OperatorUtils.getUniqueOperatorName("KafkaOutput"),
        KafkaSinglePortOutputOperator.class);
    kafkaOutput.setTopic((String)operands.get(KAFKA_TOPICS));

    Properties props = new Properties();
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_SERIALIZER);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_SERIALIZER);
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, operands.get(KAFKA_SERVERS));
    kafkaOutput.setProperties(props);

    dag.addStream(OperatorUtils.getUniqueStreamName("Formatter", "Kafka"), spec.getOutPort(), kafkaOutput.inputPort);

    return new RelInfo("Output", spec.getInputPorts(), spec.getOperator(), null, messageFormat.getRowType(typeFactory));
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory)
  {
    return messageFormat.getRowType(typeFactory);
  }
}
