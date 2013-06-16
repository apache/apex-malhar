/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.contrib.memcache;

import com.datatorrent.api.*;
import com.datatorrent.contrib.memcache.AbstractSinglePortMemcacheOutputOperator;
import com.datatorrent.api.ActivationListener;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.Context.OperatorContext;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import net.spy.memcached.*;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class MemcacheOutputOperaotorBenchmark
{
  private static final Logger logger = LoggerFactory.getLogger(MemcacheOutputOperaotorBenchmark.class);
  private static ArrayList<Object> resultList = new ArrayList<Object>();
  private static int numberOfOps=500000;
  private static int resultCount=0;

  private static final class TestMemcacheOutputOperator extends AbstractSinglePortMemcacheOutputOperator<HashMap<String, String>>
  {
    @Override
    public void processTuple(HashMap<String, String> tuple)
    {
      int exp = 60*60*24*30;
      Iterator it = tuple.entrySet().iterator();
      Entry<String, Integer> entry = (Entry)it.next();

//      OperationFuture<Boolean> of = client.set(entry.getKey(), exp, entry.getValue());
      client.set(entry.getKey(), exp, entry.getValue());
      resultCount++;
//      try {
//        if( of.get() == true )
//          ++sentTuples;
//      }
//      catch (InterruptedException ex) {
//        logger.debug(ex.toString());
//      }
//      catch (ExecutionException ex) {
//        logger.debug(ex.toString());
//      }
    }
  }

  public class MemcacheMessageReceiver
  {
    MemcachedClient client;
    ArrayList<String> servers = new ArrayList<String>();
    public HashMap<String, Integer> dataMap = new HashMap<String, Integer>();
    public int count = 0;
    public GetDataThread gdt;

    public void addSever(String server) {
      servers.add(server);
    }
    public void setup() {
      try {
      ConnectionFactory factory = new ConnectionFactoryBuilder().setOpTimeout(1000000).build();
       client = new MemcachedClient(factory, AddrUtil.getAddresses("localhost:11211"));
      }
      catch (IOException ex) {
        System.out.println(ex.toString());
      }
      gdt = new GetDataThread();
      gdt.start();
    }

    public class GetDataThread extends Thread {
      public volatile boolean working = true;
      @Override
      public void run() {
        while( working ) {
          if( resultCount < numberOfOps ) {
            try {
              Thread.sleep(10);
            }
            catch (InterruptedException ex) {
              logger.debug(ex.toString());
            }
            continue;
          }

          for( int i=0; i<numberOfOps; i++ ) {
            Object o = client.asyncGet(Integer.toString(i));
            resultList.add(o);
          }
          break;
        }
      }
    }
  }

  public static class SourceModule extends BaseOperator
  implements InputOperator, ActivationListener<OperatorContext>
  {
    public final transient DefaultOutputPort<HashMap<String, String>> outPort = new DefaultOutputPort<HashMap<String, String>>(this);
    static transient ArrayBlockingQueue<HashMap<String, String>> holdingBuffer;

    @Override
    public void setup(OperatorContext context)
    {
      holdingBuffer = new ArrayBlockingQueue<HashMap<String, String>>(1024 * 1024);
    }

    public void emitTuple(HashMap<String, String> message)
    {
      outPort.emit(message);
    }

    @Override
    public void emitTuples()
    {
      for (int i = holdingBuffer.size(); i-- > 0;) {
        emitTuple(holdingBuffer.poll());
      }
    }

    @Override
    public void activate(OperatorContext ctx)
    {
      for (int i = 0; i < numberOfOps; i++) {
          HashMap<String, String> map  = new HashMap<String, String>();
          map.put(Integer.toString(i), "Hello this is a test " + i);
          holdingBuffer.add(map);
      }
    }

    @Override
    public void deactivate()
    {
    }

    public void replayTuples(long windowId)
    {
    }
  }

  @Test
  public void testDag() throws Exception {
    String server = "localhost:11211";

    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    SourceModule source = dag.addOperator("source", SourceModule.class);
    final TestMemcacheOutputOperator producer = dag.addOperator("producer", new TestMemcacheOutputOperator());
    producer.addServer(server);
    dag.addStream("Stream", source.outPort, producer.inputPort).setInline(true);

    final MemcacheMessageReceiver consumer = new MemcacheMessageReceiver();
    consumer.addSever(server);

    final LocalMode.Controller lc = lma.getController();
    lc.runAsync();

    long start = System.currentTimeMillis();
    try {
      while( resultCount < numberOfOps ) {
        Thread.sleep(100);
      }
    }
    catch (InterruptedException ex) {
    }
    lc.shutdown();

    long end = System.currentTimeMillis();
    long time = end - start;
    consumer.setup();
    consumer.gdt.join();

    System.out.println("processed "+numberOfOps+" tuples in DAG used "+time+" ms or "+numberOfOps*1.0/time*1000.0+" ops resultCount:"+resultCount);
    Assert.assertEquals("Number of emitted tuples", numberOfOps, resultList.size());
    int i=0;
    for( Object o : resultList ) {
      Future f = (Future)o;
      String str = (String)f.get();
      Assert.assertEquals("value of "+i+" is ", str, "Hello this is a test " + i);
      i++;
    }
    System.out.println("resultCount:"+resultCount+" i:"+i);
  }

  @Ignore
  @Test
    public void testSimple2() throws IOException, InterruptedException, ExecutionException, TimeoutException {
//    ConnectionFactory factory = new BinaryConnectionFactory();
      ConnectionFactory factory = new ConnectionFactoryBuilder().setOpTimeout(1000000).build();
       MemcachedClient client = new MemcachedClient(factory, AddrUtil.getAddresses("localhost:11211"));
       client.flush();

       int numberOfOps = 1000000;
        long time1 = System.nanoTime();
        for (int i = 0; i < numberOfOps; i++) {
            client.set(Integer.toString(i), 86400, "Hello this is a test " + i);
        }

        long time2 = System.nanoTime();
        System.out.println("Time to set data = " + ((time2 - time1) / 1000000.0) + "ms ");

        client.waitForQueues(86400, TimeUnit.SECONDS);
        long time3 = System.nanoTime();
        System.out.println("Time for queues to drain 1 = " + ((time3 - time2) / 1000000.0) + "ms  Set data speed:"+numberOfOps/(time3-time1)*1000000000.0);

        long time4 = System.nanoTime();
        ArrayList<Future> list = new ArrayList<Future>();
        for( int i=0; i<numberOfOps; i++ ) {
          Future f = client.asyncGet(Integer.toString(i));
          list.add(f);
        }
        long time5 = System.nanoTime();
        System.out.println("Time to get future = " + ((time5 - time4) / 1000000.0) + "ms");
        for( Future f : list ) {
          Object o = f.get(1, TimeUnit.SECONDS);
        }
        long time6 = System.nanoTime();
        System.out.println("Time to get data = " + ((time6 - time5) / 1000000.0) + "ms Get data speed:"+numberOfOps/(time6-time4)*1000000000.0);

        client.shutdown(2000, TimeUnit.MILLISECONDS);
    }
}
