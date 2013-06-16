/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.contrib.memcache_whalin;

import com.datatorrent.api.ActivationListener;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.InputOperator;
import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Memcache input adapter operator, which get data from Memcached using whalin library<p><br>
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: No input port<br>
 * <b>Output</b>: Can have any number of output ports<br>
 * <br>
 * Properties:<br>
 * <b>tuple_blast</b>: Number of tuples emitted in each burst<br>
 * <b>bufferSize</b>: Size of holding buffer<br>
 * <b>servers</b>: the kestrel server url list<br>
 * <b>keys</b>: the queueName to interact with kestrel server<br>
 * <br>
 * Compile time checks:<br>
 * Class derived from this has to implement the abstract method emitTuple() <br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * <b>Benchmarks</b>: TBD
 * <br>
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public abstract class AbstractMemcacheInputOperator implements InputOperator, ActivationListener<OperatorContext>
{
  private static final int DEFAULT_BLAST_SIZE = 1000;
  private static final int DEFAULT_BUFFER_SIZE = 1024 * 1024;
  private int tuple_blast = DEFAULT_BLAST_SIZE;
  private int bufferSize = DEFAULT_BUFFER_SIZE;
  transient ArrayBlockingQueue<Object> holdingBuffer;
  private String[] servers;
  private String[] keys;
  protected transient MemCachedClient mcc;
  private transient SockIOPool pool;
  public abstract void emitTuple(Object obj);
  public transient GetDataThread gqt;

/**
 * a thread actively pulling data from Memcached
 * and added to the holdingBuffer
 */
  private class GetDataThread extends Thread
  {
    @Override
    public void run()
    {
      Object[] objs = mcc.getMultiArray(keys);
      for( Object o : objs ) {
        holdingBuffer.add(o);
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
    holdingBuffer = new ArrayBlockingQueue<Object>(bufferSize);
    pool = SockIOPool.getInstance();
    pool.setServers( servers );
    //		pool.setWeights( weights );

    // set some basic pool settings
    // 5 initial, 5 min, and 250 max conns
    // and set the max idle time for a conn
    // to 6 hours
    pool.setInitConn( 5 );
    pool.setMinConn( 5 );
    pool.setMaxConn( 250 );
    pool.setMaxIdle( 1000 * 60 * 60 * 6 );

    // set the sleep for the maint thread
    // it will wake up every x seconds and
    // maintain the pool size
    pool.setMaintSleep( 30 );

    // set some TCP settings
    // disable nagle
    // set the read timeout to 3 secs
    // and don't set a connect timeout
    pool.setNagle( false );
    pool.setSocketTO( 3000 );
    pool.setSocketConnectTO( 0 );

    pool.initialize();

    mcc = new MemCachedClient();
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void activate(OperatorContext ctx)
  {
    gqt = new GetDataThread();
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

  public void setServers(String[] servers)
  {
    this.servers = servers;
  }

  public void setKeys(String[] keys) {
    this.keys = keys;
  }
}
