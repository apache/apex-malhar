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
package org.apache.apex.malhar.contrib.zmq;


import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Operator.ActivationListener;
import com.datatorrent.common.util.BaseOperator;

/**
 * This is the base implementation of a ZeroMQ input operator.&nbsp;
 * This operator will behave like a subscriber that issues requests.&nbsp;
 * Subclasses should implement the methods which convert ZeroMQ messages into tuples.
 * <p>
 * <br>
 * Ports:<br>
 * <b>Input</b>: No input port<br>
 * <b>Output</b>: Can have any number of output ports<br>
 * <br>
 * Properties:<br>
 * <b>tuple_blast</b>: Number of tuples emitted in each burst<br>
 * <b>bufferSize</b>: Size of holding buffer<br>
 * <b>url</b>:the url for the subscriber to connect to ZeroMQ publisher<br>
 * <b>syncUrl</b>: the url for the subscriber to synchronize with publisher<br>
 * <b>filter</b>: the filter that subscriber wants to subscribe, default is ""<br>
 * <br>
 * Compile time checks:<br>
 * Class derived from this has to implement the abstract method emitTuple() <br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for AbstractBaseZeroMQInputOperator&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>400 thousand K,V pairs/s</td><td>One tuple per key per window per port</td><td>In-bound rate is the main determinant of performance. Operator can emit about 400 thousand unique (k,v immutable pairs) tuples/sec as ZeroMQ DAG. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may differ</td></tr>
 * </table><br>
 * <br>
 * </p>
 * @displayName Abstract Base ZeroMQ Input
 * @category Messaging
 * @tags input operator
 * @since 0.3.2
 */
public abstract class AbstractBaseZeroMQInputOperator extends BaseOperator implements InputOperator, ActivationListener<OperatorContext>
{
  @SuppressWarnings("unused")
  private static final Logger logger = LoggerFactory.getLogger(AbstractBaseZeroMQInputOperator.class);
  protected transient ZMQ.Context context;
  protected transient ZMQ.Socket subscriber;
  protected transient ZMQ.Socket syncclient;
  private String url;
  private String syncUrl;
  private String filter = "";

  private static final int DEFAULT_BLAST_SIZE = 1000;
  private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
  private int tuple_blast = DEFAULT_BLAST_SIZE;
  private int bufferSize = DEFAULT_BUFFER_SIZE;
  private volatile boolean running = false;
  transient ArrayBlockingQueue<byte[]> holdingBuffer = new ArrayBlockingQueue<byte[]>(bufferSize);

  public void setUrl(String url)
  {
    this.url = url;
  }

  public void setSyncUrl(String url)
  {
    this.syncUrl = url;
  }

  public void setFilter(String filter)
  {
    this.filter = filter;
  }

//  @Min(1)
  public void setTupleBlast(int i)
  {
    this.tuple_blast = i;
  }

  public void setBufferSize(int size)
  {
    this.bufferSize = size;
  }

  @Override
  public void setup(OperatorContext ctx)
  {
    context = ZMQ.context(1);
    subscriber = context.socket(ZMQ.SUB);
    subscriber.connect(url);
    subscriber.subscribe(filter.getBytes());
    syncclient = context.socket(ZMQ.REQ);
    syncclient.connect(syncUrl);
    syncclient.send("".getBytes(), 0);
  }

  @Override
  public void teardown()
  {
    subscriber.close();
    syncclient.close();
    context.term();
  }

/**
 * start a thread receiving data,
 * and add into holdingBuffer
 * @param ctx
 */
  @Override
  public void activate(OperatorContext ctx)
  {
    new Thread()
    {
      @Override
      public void run()
      {
        running = true;
        while (running) {
          try {
            byte[] message = subscriber.recv(0);
            if (message != null) {
              holdingBuffer.add(message);
            }
          } catch (Exception e) {
//        logger.debug(e.toString());
            break;
          }
        }
      }
    }.start();
  }

  @Override
  public void deactivate()
  {
    running = false;
  }

  public abstract void emitTuple(byte[] message);

  @Override
  public void emitTuples()
  {
    int ntuples = tuple_blast;
    if (ntuples > holdingBuffer.size()) {
      ntuples = holdingBuffer.size();
    }
    for (int i = ntuples; i-- > 0;) {
      byte[] msg = holdingBuffer.poll();
      if (msg == null) {
        break;
      }
      emitTuple(msg);
    }
  }
}
