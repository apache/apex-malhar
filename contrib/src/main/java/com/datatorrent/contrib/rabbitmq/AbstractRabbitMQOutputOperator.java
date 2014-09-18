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
package com.datatorrent.contrib.rabbitmq;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is an abstract RabbitMQ output adapter operator.
 * <p>
 * <br>
 * Ports:<br>
 * <b>Input</b>: Can have any number of input ports<br>
 * <b>Output</b>: no output port<br>
 * <br>
 * Properties:<br>
 * <b>host</b>:the address for the consumer to connect <br>
 * <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for AbstractRabbitMQOutputOperator&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td>One tuple per key per window per port</td><td><b>10 thousand K,V pairs/s</td><td>Out-bound rate is the main determinant of performance. Operator can process about 10 thousand unique (k,v immutable pairs) tuples/sec as RabbitMQ DAG. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may differ</td></tr>
 * </table><br>
 * <br>
 * </p>
 * @displayName Abstract RabbitMQ Output
 * @category messaging
 * @tags output
 *
 * @since 0.3.2
 */
public class AbstractRabbitMQOutputOperator extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractRabbitMQInputOperator.class);
  transient ConnectionFactory connFactory = new ConnectionFactory();
  transient QueueingConsumer consumer = null;
  transient Connection connection = null;
  transient Channel channel = null;
  transient String exchange = "testEx";
  transient String queueName="testQ";

  @Override
  public void setup(OperatorContext context)
  {
    try {
      connFactory.setHost("localhost");
      connection = connFactory.newConnection();
      channel = connection.createChannel();
      channel.exchangeDeclare(exchange, "fanout");
//      channel.queueDeclare(queueName, false, false, false, null);
    }
    catch (IOException ex) {
      logger.debug(ex.toString());
    }
  }

  public void setQueueName(String queueName) {
    this.queueName = queueName;
  }

  public void setExchange(String exchange) {
    this.exchange = exchange;
  }
  @Override
  public void teardown()
  {
    try {
      channel.close();
      connection.close();
    }
    catch (IOException ex) {
      logger.debug(ex.toString());
    }
  }
}
