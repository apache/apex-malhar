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
package com.datatorrent.apps.logstream;

import java.util.*;

import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.apps.logstream.PropertyRegistry.LogstreamPropertyRegistry;
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.contrib.rabbitmq.AbstractSinglePortRabbitMQInputOperator;

/**
 *
 * Input operator to consume logs messages from RabbitMQ
 * This operator is partitionable, each partition will receive messages from the routing key its assigned.
 */
public class RabbitMQLogsInputOperator extends AbstractSinglePortRabbitMQInputOperator<byte[]> implements Partitioner<RabbitMQLogsInputOperator>
{
  private static final Logger logger = LoggerFactory.getLogger(RabbitMQLogsInputOperator.class);
  private String[] routingKeys;
  private LogstreamPropertyRegistry registry;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    LogstreamPropertyRegistry.setInstance(registry);
  }

  @Override
  public byte[] getTuple(byte[] message)
  {
    if (registry == null) {
      return message;
    }
    String inputString = new String(message);
    try {
      JSONObject jSONObject = new JSONObject(inputString);
      int typeId = registry.getIndex(LogstreamUtil.LOG_TYPE, routingKey);
      jSONObject.put(LogstreamUtil.LOG_TYPE, typeId);
      String outputString = jSONObject.toString();
      message = outputString.getBytes();
    }
    catch (Throwable ex) {
      DTThrowable.rethrow(ex);
    }

    return message;
  }

  /**
   * Supply the properties to the operator.
   * The properties include hostname, exchange, exchangeType and colon separated routing keys specified in the following format
   * hostName[:port], exchange, exchangeType, queueName, routingKey1[:routingKey2]
   *
   * The queue name is assumed to be same as routing key
   *
   * @param props
   */
  public void addPropertiesFromString(String[] props)
  {
    try {
      //input string format
      //host, exchange, exchangeType, routingKey1:routingKey2:routingKey3
      //eg: localhost:5672, logstash, direct, apache:mysql:syslog
      if (props[0].contains(":")){
        String[] split = props[0].split(":");
        host = split[0];
        port = new Integer(split[1]);
      } else {
        host = props[0];
      }

      exchange = props[1];
      exchangeType = props[2];

      if (props[3] != null) {
        routingKeys = props[3].split(":");
        for (String rKey : routingKeys) {
          registry.bind(LogstreamUtil.LOG_TYPE, rKey);
        }
      }
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
   * supply the registry object which is used to store and retrieve meta information about each tuple
   *
   * @param registry
   */
  public void setRegistry(LogstreamPropertyRegistry registry)
  {
    this.registry = registry;
  }

  /**
   * Partitions count will be the number of input routing keys.
   * Each partition receives tuples from its routing key.
   *
   * @param partitions
   * @param incrementalCapacity
   * @return
   */
  @Override
  public Collection<Partition<RabbitMQLogsInputOperator>> definePartitions(Collection<Partition<RabbitMQLogsInputOperator>> partitions, int incrementalCapacity)
  {
    if (routingKeys == null || routingKeys.length == 0) {
      return partitions;
    }

    ArrayList<Partition<RabbitMQLogsInputOperator>> newPartitions = new ArrayList<Partition<RabbitMQLogsInputOperator>>();
    for (String rKey : routingKeys) {
      try {
        RabbitMQLogsInputOperator oper = new RabbitMQLogsInputOperator();
        oper.host = host;
        oper.port = port;
        oper.exchange = exchange;
        oper.exchangeType = exchangeType;
        oper.registry = registry;
        oper.routingKeys = routingKeys;
        oper.routingKey = rKey;
        oper.queueName = rKey;

        Partition<RabbitMQLogsInputOperator> partition = new DefaultPartition<RabbitMQLogsInputOperator>(oper);
        newPartitions.add(partition);
      }
      catch (Throwable ex) {
        DTThrowable.rethrow(ex);
      }
    }
    return newPartitions;
  }

  @Override
  public void partitioned(Map<Integer, com.datatorrent.api.Partitioner.Partition<RabbitMQLogsInputOperator>> partitions)
  {
  }

}
