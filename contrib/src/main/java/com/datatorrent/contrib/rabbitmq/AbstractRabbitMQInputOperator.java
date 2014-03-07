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

import com.datatorrent.api.*;
import com.datatorrent.api.ActivationListener;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.annotation.ShipContainingJars;
import com.rabbitmq.client.*;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RabbitMQ input adapter operator, which consume data from RabbitMQ message bus.<p><br>
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: No input port<br>
 * <b>Output</b>: Can have any number of output ports<br>
 * <br>
 * Properties:<br>
 * <b>tuple_blast</b>: Number of tuples emitted in each burst<br>
 * <b>bufferSize</b>: Size of holding buffer<br>
 * <b>host</b>:the address for the consumer to connect to rabbitMQ producer<br>
 * <b>exchange</b>:the exchange for the consumer to connect to rabbitMQ producer<br>
 * <b>exchangeType</b>:the exchangeType for the consumer to connect to rabbitMQ producer<br>
 * <b>routingKey</b>:the routingKey for the consumer to connect to rabbitMQ producer<br>
 * <b>queueName</b>:the queueName for the consumer to connect to rabbitMQ producer<br>
 * <br>
 * Compile time checks:<br>
 * Class derived from this has to implement the abstract method emitTuple() <br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for AbstractRabbitMQInputOperator&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>10 thousand K,V pairs/s</td><td>One tuple per key per window per port</td><td>In-bound rate is the main determinant of performance. Operator can emit about 10 thousand unique (k,v immutable pairs) tuples/sec as RabbitMQ DAG. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may differ</td></tr>
 * </table><br>
 * <br>
 *
 * @since 0.3.2
 */
@ShipContainingJars(classes={com.rabbitmq.client.ConnectionFactory.class})
public abstract class AbstractRabbitMQInputOperator<T>
    implements InputOperator,
ActivationListener<OperatorContext>
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractRabbitMQInputOperator.class);
  @NotNull
  protected String host;
  protected int port;
  @NotNull
  protected String exchange;
  @NotNull
  protected String exchangeType;
  protected String routingKey = "";
  protected String queueName; // Has to be supplied by client when exchangeType is not "fanout"
  protected transient ConnectionFactory connFactory;
//  QueueingConsumer consumer = null;

  private static final int DEFAULT_BLAST_SIZE = 1000;
  private static final int DEFAULT_BUFFER_SIZE = 1024*1024;
  private int tuple_blast = DEFAULT_BLAST_SIZE;
  protected int bufferSize = DEFAULT_BUFFER_SIZE;

  protected transient Connection connection;
  protected transient Channel channel;
  protected transient TracingConsumer tracingConsumer;
  protected transient String cTag;
  protected transient ArrayBlockingQueue<byte[]> holdingBuffer;

/**
 * define a consumer which can asynchronously receive data,
 * and added to holdingBuffer
 */
 public class TracingConsumer extends DefaultConsumer
  {
    public TracingConsumer(Channel ch)
    {
      super(ch);
    }

    @Override
    public void handleConsumeOk(String c)
    {
      logger.debug(this + ".handleConsumeOk(" + c + ")");
      super.handleConsumeOk(c);
    }

    @Override
    public void handleCancelOk(String c)
    {
      logger.debug(this + ".handleCancelOk(" + c + ")");
      super.handleCancelOk(c);
    }

    @Override
    public void handleShutdownSignal(String c, ShutdownSignalException sig)
    {
      logger.debug(this + ".handleShutdownSignal(" + c + ", " + sig + ")");
      super.handleShutdownSignal(c, sig);
    }

    @Override
    public void handleDelivery(String consumer_Tag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException
    {
      holdingBuffer.add(body);
//      logger.debug("Received Async message:" + new String(body)+" buffersize:"+holdingBuffer.size());
    }
  }

  @Override
  public void emitTuples()
  {
    int ntuples = tuple_blast;
    if (ntuples > holdingBuffer.size()) {
      ntuples = holdingBuffer.size();
    }
    for (int i = ntuples; i-- > 0;) {
      emitTuple(holdingBuffer.poll());
    }
  }

  public abstract void emitTuple(byte[] message);

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    holdingBuffer = new ArrayBlockingQueue<byte[]>(bufferSize);
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void activate(OperatorContext ctx)
  {
    try {
      connFactory = new ConnectionFactory();
      connFactory.setHost(host);
      if (port != 0){
        connFactory.setPort(port);
      }

      connection = connFactory.newConnection();
      channel = connection.createChannel();

      channel.exchangeDeclare(exchange, exchangeType);
      if (queueName == null){
        // unique queuename is generated
        // used in case of fanout exchange
        queueName = channel.queueDeclare().getQueue();
      } else {
        // user supplied name
        // used in case of direct exchange
        channel.queueDeclare(queueName, true, false, false, null);
      }

      channel.queueBind(queueName, exchange, routingKey);

//      consumer = new QueueingConsumer(channel);
//      channel.basicConsume(queueName, true, consumer);
      tracingConsumer = new TracingConsumer(channel);
      cTag = channel.basicConsume(queueName, true, tracingConsumer);
    }
    catch (IOException ex) {
      throw new RuntimeException("Connection Failure", ex);
    }
  }

  @Override
  public void deactivate()
  {
    try {
      channel.close();
      connection.close();
    }
    catch (IOException ex) {
      logger.debug(ex.toString());
    }
  }
  public void setTupleBlast(int i)
  {
    this.tuple_blast = i;
  }

  public String getHost()
  {
    return host;
  }

  public void setHost(String host)
  {
    this.host = host;
  }

  public int getPort()
  {
    return port;
  }

  public void setPort(int port)
  {
    this.port = port;
  }

  public String getExchange()
  {
    return exchange;
  }

  public void setExchange(String exchange)
  {
    this.exchange = exchange;
  }

  public String getQueueName()
  {
    return queueName;
  }

  public void setQueueName(String queueName)
  {
    this.queueName = queueName;
  }

  public String getExchangeType()
  {
    return exchangeType;
  }

  public void setExchangeType(String exchangeType)
  {
    this.exchangeType = exchangeType;
  }

  public String getRoutingKey()
  {
    return routingKey;
  }

  public void setRoutingKey(String routingKey)
  {
    this.routingKey = routingKey;
  }

}
