/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.rabbitmq;

import com.datatorrent.api.*;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.IdempotentStorageManager;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.netlet.util.DTThrowable;
import com.rabbitmq.client.*;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is the base implementation of a RabbitMQ input operator.&nbsp;
 * Subclasses should implement the methods which convert RabbitMQ messages to tuples.
 * <p>
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
 * </p>
 * @displayName Abstract RabbitMQ Input
 * @category Messaging
 * @tags input operator
 *
 * @since 0.3.2
 */
public abstract class AbstractRabbitMQInputOperator<T> implements
    InputOperator, Operator.ActivationListener<OperatorContext>,
    Operator.CheckpointListener
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
  
  protected transient ArrayBlockingQueue<KeyValPair<Long,byte[]>> holdingBuffer;
  private IdempotentStorageManager idempotentStorageManager;
  protected final transient Map<Long, byte[]> currentWindowRecoveryState;
  private transient final Set<Long> pendingAck;
  private transient final Set<Long> recoveredTags;
  private transient long currentWindowId;
  private transient int operatorContextId;
  
  public AbstractRabbitMQInputOperator()
  {
    currentWindowRecoveryState = new HashMap<Long, byte[]>();
    pendingAck = new HashSet<Long>();
    recoveredTags = new HashSet<Long>();
  }

  
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
      long tag = envelope.getDeliveryTag();
      if(envelope.isRedeliver() && (recoveredTags.contains(tag) || pendingAck.contains(tag)))
      {
        if(recoveredTags.contains(tag)) {
          pendingAck.add(tag);
        }
        return;
      }
      
      // Acknowledgements are sent at the end of the window after adding to idempotency manager
      pendingAck.add(tag);
      holdingBuffer.add(new KeyValPair<Long, byte[]>(tag, body));
      logger.debug("Received Async message: {}  buffersize: {} ", new String(body), holdingBuffer.size());
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
      KeyValPair<Long, byte[]> message =  holdingBuffer.poll();
      currentWindowRecoveryState.put(message.getKey(), message.getValue());
      emitTuple(message.getValue());
    }
  }

  public abstract void emitTuple(byte[] message);

  @Override
  public void beginWindow(long windowId)
  {
    currentWindowId = windowId;
    if (windowId <= this.idempotentStorageManager.getLargestRecoveryWindow()) {
      replay(windowId);
    }
  }

  @SuppressWarnings("unchecked")
  private void replay(long windowId) {      
    Map<Long, byte[]> recoveredData;
    try {
      recoveredData = (Map<Long, byte[]>) this.idempotentStorageManager.load(operatorContextId, windowId);
      if (recoveredData == null) {
        return;
      }
      for (Entry<Long, byte[]>  recoveredEntry : recoveredData.entrySet()) {
        recoveredTags.add(recoveredEntry.getKey());
        emitTuple(recoveredEntry.getValue());
      }
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
  }

  
  @Override
  public void endWindow()
  {
    //No more messages can be consumed now. so we will call emit tuples once more
    //so that any pending messages can be emitted.
    KeyValPair<Long, byte[]> message;
    while ((message = holdingBuffer.poll()) != null) {
      currentWindowRecoveryState.put(message.getKey(), message.getValue());
      emitTuple(message.getValue());      
    }
    
    try {
      this.idempotentStorageManager.save(currentWindowRecoveryState, operatorContextId, currentWindowId);
    } catch (IOException e) {
      DTThrowable.rethrow(e);
    }
    
    currentWindowRecoveryState.clear();
    
    for (Long deliveryTag : pendingAck) {
      try {
        channel.basicAck(deliveryTag, false);
      } catch (IOException e) {        
        DTThrowable.rethrow(e);
      }
    }
    
    pendingAck.clear();
  }

  @Override
  public void setup(OperatorContext context)
  {
    this.operatorContextId = context.getId();
    holdingBuffer = new ArrayBlockingQueue<KeyValPair<Long, byte[]>>(bufferSize);
    this.idempotentStorageManager.setup(context);
  }

  @Override
  public void teardown()
  {
    this.idempotentStorageManager.teardown();
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
      boolean resetQueueName = false;
      if (queueName == null){
        // unique queuename is generated
        // used in case of fanout exchange
        queueName = channel.queueDeclare().getQueue();
        resetQueueName = true;
      } else {
        // user supplied name
        // used in case of direct exchange
        channel.queueDeclare(queueName, true, false, false, null);
      }

      channel.queueBind(queueName, exchange, routingKey);

//      consumer = new QueueingConsumer(channel);
//      channel.basicConsume(queueName, true, consumer);
      tracingConsumer = new TracingConsumer(channel);
      cTag = channel.basicConsume(queueName, false, tracingConsumer);
      if(resetQueueName)
      {
        queueName = null;
      }
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

  @Override
  public void checkpointed(long windowId)
  {
  }

  @Override
  public void committed(long windowId)
  {
    try {
      idempotentStorageManager.deleteUpTo(operatorContextId, windowId);
    }
    catch (IOException e) {
      throw new RuntimeException("committing", e);
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
  
  public IdempotentStorageManager getIdempotentStorageManager() {
    return idempotentStorageManager;
  }
  
  public void setIdempotentStorageManager(IdempotentStorageManager idempotentStorageManager) {
    this.idempotentStorageManager = idempotentStorageManager;
  }
  


}
