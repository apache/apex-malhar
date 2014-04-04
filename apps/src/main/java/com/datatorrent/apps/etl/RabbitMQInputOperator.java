/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.apps.etl;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.annotation.Nonnull;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;

import com.datatorrent.contrib.rabbitmq.AbstractSinglePortRabbitMQInputOperator;
import com.datatorrent.lib.datamodel.converter.Converter;

/**
 * @param <T> output tuple type
 */
public class RabbitMQInputOperator<T> extends AbstractSinglePortRabbitMQInputOperator<T> implements Partitioner<RabbitMQInputOperator<T>>
{
  @Nonnull
  private String logTypes;
  @Nonnull
  Converter<JSONObject, T> converter;

  @Override
  public T getTuple(byte[] message)
  {
    try {
      JSONObject jsonObj = new JSONObject(new String(message));
      jsonObj.put(Constants.LOG_TYPE, routingKey);
      return converter.convert(jsonObj);
    }
    catch (JSONException e) {
      throw new RuntimeException("creating json", e);
    }
  }

  public void setLogTypes(@Nonnull String logTypes)
  {
    Preconditions.checkArgument(logTypes.length() > 0, "logTypes");
    this.logTypes = logTypes;
  }

  public void setConverter(@Nonnull Converter<JSONObject, T> converter)
  {
    this.converter = converter;
  }

  @Override
  public Collection<Partition<RabbitMQInputOperator<T>>> definePartitions(Collection<Partition<RabbitMQInputOperator<T>>> partitions, int i)
  {
    String[] logs = logTypes.split(":");
    List<Partition<RabbitMQInputOperator<T>>> newPartitions = Lists.newArrayList();
    for (String type : logs) {
      RabbitMQInputOperator<T> oper = new RabbitMQInputOperator<T>();
      oper.host = host;
      oper.port = port;
      oper.exchange = exchange;
      oper.exchangeType = exchangeType;
      oper.routingKey = type;
      oper.queueName = type;
      oper.logTypes = logTypes;
      oper.converter = converter;

      DefaultPartition<RabbitMQInputOperator<T>> partition = new DefaultPartition<RabbitMQInputOperator<T>>(oper);
      newPartitions.add(partition);
    }
    return newPartitions;
  }

  @Override
  public void partitioned(Map<Integer, Partition<RabbitMQInputOperator<T>>> integerPartitionMap)
  {
  }
}
