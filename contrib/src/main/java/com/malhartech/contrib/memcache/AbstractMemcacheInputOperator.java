/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.memcache;

import com.malhartech.api.ActivationListener;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.InputOperator;
import com.malhartech.util.CircularBuffer;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactory;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Memcache input adapter operator, which get data from Memcached using spymemcached library<p><br>
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: No input port<br>
 * <b>Output</b>: Can have any number of output ports<br>
 * <br>
 * Properties:<br>
 * <b>tuple_blast</b>: Number of tuples emitted in each burst<br>
 * <b>bufferSize</b>: Size of holding buffer<br>
 * <b>servers</b>: the memcache server url list<br>
 * <b>keys</b>: the queueName to interact with memcache server<br>
 * <br>
 * Compile time checks:<br>
 * Class derived from this has to implement the abstract method emitTuple() <br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for AbstractMemcacheInputOperator&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td><b>34 thousand K,V pairs/s</td><td>One tuple per key per window per port</td><td>In-bound rate is the main determinant of performance. Operator can emit about 34 thousand unique (k,v immutable pairs) tuples/sec as Memcache DAG. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may differ</td></tr>
 * </table><br>
 * <br>
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public abstract class AbstractMemcacheInputOperator implements InputOperator, ActivationListener<OperatorContext>
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractMemcacheInputOperator.class);
  private static final int DEFAULT_BLAST_SIZE = 1000;
  private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
  private int tuple_blast = DEFAULT_BLAST_SIZE;
  private int bufferSize = DEFAULT_BUFFER_SIZE;
  protected transient CircularBuffer<Object> holdingBuffer;
  protected transient MemcachedClient client;
  private ArrayList<String> servers = new ArrayList<String>();
  private ArrayList<String> keys = new ArrayList<String>();
  public Thread getDataThread;
  private GetDataRunnable runnable = new GetDataRunnable();
//  private Runnable runnable = new GetDataRunnable();

  public abstract void emitTuple(Object o);

  public void addServer(String server) {
    servers.add(server);
  }

  public void addKey(String key) {
    keys.add(key);
  }

  public void setRunnable(GetDataRunnable r) {
    runnable = r;
  }

  public void runFunction() {
        Map<String, Object> map = client.getBulk(keys);
        holdingBuffer.add(map);
        System.out.println("run Ssssssssuper function");
  }

/**
 * a thread actively pulling data from Memcached
 * and add to the holdingBuffer, user can write their own Runnable interface
 * to retreive desired data
 */

  public static class GetDataRunnable implements Runnable
  {
    AbstractMemcacheInputOperator op;
    public void setOp(AbstractMemcacheInputOperator op) {
      this.op = op;
    }
    @Override
    public void run()
    {
      op.runFunction();
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
      emitTuple(holdingBuffer.pollUnsafe());
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
    holdingBuffer = new CircularBuffer<Object>(bufferSize);
    try {
    ConnectionFactory factory = new ConnectionFactoryBuilder().setOpTimeout(1000000).build();
      client = new MemcachedClient(factory, AddrUtil.getAddresses(servers));
//      client = new MemcachedClient(AddrUtil.getAddresses(servers));
    }
    catch (IOException ex) {
      logger.info(ex.toString());
    }
  }

  @Override
  public void teardown()
  {
    client.shutdown(2000, TimeUnit.MILLISECONDS);
  }

  @Override
  public void activate(OperatorContext ctx)
  {
    runnable.setOp(this);
    getDataThread = new Thread(runnable);
    getDataThread.start();
  }

  @Override
  public void deactivate()
  {
  }

  public void setTupleBlast(int i)
  {
    this.tuple_blast = i;
  }
}
