/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.kestrel;

import com.datatorrent.api.ActivationListener;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.InputOperator;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kestrel input adapter operator, which consume data from Kestrel message bus.<p><br>
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: No input port<br>
 * <b>Output</b>: Can have any number of output ports<br>
 * <br>
 * Properties:<br>
 * <b>tuple_blast</b>: Number of tuples emitted in each burst<br>
 * <b>bufferSize</b>: Size of holding buffer<br>
 * <b>queueName</b>: the queueName to interact with kestrel server<br>
 * <b>servers</b>: the kestrel server url list<br>
 * <br>
 * Compile time checks:<br>
 * Class derived from this has to implement the abstract method emitTuple() <br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for AbstractKestrelMQInputOperator&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>10 thousand K,V pairs/s</td><td>One tuple per key per window per port</td><td>In-bound rate is the main determinant of performance. Operator can emit about 1 thousand unique (k,v immutable pairs) tuples/sec as Kestrel DAG. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may differ</td></tr>
 * </table><br>
 * <br>
 *
 */
public abstract class AbstractKestrelInputOperator implements InputOperator, ActivationListener<OperatorContext>
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractKestrelInputOperator.class);
  private static final int DEFAULT_BLAST_SIZE = 1000;
  private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
  private int tuple_blast = DEFAULT_BLAST_SIZE;
  private int bufferSize = DEFAULT_BUFFER_SIZE;
  transient ArrayBlockingQueue<byte[]> holdingBuffer;
  private String queueName;
  private String[] servers;
  private transient MemcachedClient mcc;
  private transient SockIOPool pool;
  public abstract void emitTuple(byte[] message);

/**
 * a thread actively pulling data from Kestrel bus,
 * and added to the holdingBuffer
 */
  private class GetQueueThread extends Thread
  {
    @Override
    public void run()
    {
      while (true) {
        byte[] result = (byte[])mcc.get(queueName);
        if (result != null) {
          holdingBuffer.add(result);
        }
//        try {
//          Thread.sleep(10);
//        }
//        catch (InterruptedException ex) {
//          logger.debug(ex.toString());
//        }
      }
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
    pool = SockIOPool.getInstance();
    pool.setServers(servers);
    pool.setFailover(true);
    pool.setInitConn(10);
    pool.setMinConn(5);
    pool.setMaxConn(250);
    pool.setMaintSleep(30);
    pool.setNagle(false);
    pool.setSocketTO(3000);
    pool.setAliveCheck(true);
    pool.initialize();

    mcc = new MemcachedClient();
    GetQueueThread gqt = new GetQueueThread();
    gqt.start();
  }

  @Override
  public void deactivate()
  {
    pool.shutDown();
  }

  public void setTupleBlast(int i)
  {
    this.tuple_blast = i;
  }

  public void setQueueName(String name)
  {
    queueName = name;
  }

  public void setServers(String[] servers)
  {
    this.servers = servers;
  }
}
