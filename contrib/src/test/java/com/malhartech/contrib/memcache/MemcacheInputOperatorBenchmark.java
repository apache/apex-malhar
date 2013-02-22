/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.memcache;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DAG;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.stram.StramLocalCluster;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class MemcacheInputOperatorBenchmark
{
  private static final Logger logger = LoggerFactory.getLogger(MemcacheInputOperatorBenchmark.class);
  private static ArrayList<Object> resultList = new ArrayList<Object>();
  private static int resultCount=0;
  private static int numberOfOps=500000;

  public static class TestMemcacheInputOperator extends AbstractSinglePortMemcacheInputOperator<Object>
  {

    @Override
    public void runFunction() {
        for( int i=0; i<numberOfOps; i++ ) {
          Future f = client.asyncGet(Integer.toString(i));
          holdingBuffer.add(f);
        }
    }

    @Override
    public Object getTuple(Object o)
    {
      Future f = (Future)o;
      Object obj=null;
      try {
        obj = f.get();
      }
      catch (InterruptedException ex) {
        logger.debug(ex.toString());
      }
      catch (ExecutionException ex) {
        logger.debug(ex.toString());
      }
      return obj;
    }

    public void generateData()
    {
      for (int i = 0; i < numberOfOps; i++) {
        client.set(Integer.toString(i), 86400, "Hello this is a test " + i);
      }
    }
  }

  public static class CollectorModule<T> extends BaseOperator
  {
    public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>(this){

      @Override
      public void process(T t)
      {
//        resultList.add((Future)t);
        resultList.add(t);
        resultCount++;
      }
    };
  }

  @Test
  public void testInputOperator() throws InterruptedException, Exception {
    String server = "localhost:11211";
    TestMemcacheInputOperator gen = new TestMemcacheInputOperator();
    gen.addServer(server);
    gen.setup(null);
    gen.generateData();
    gen.teardown();

    DAG dag = new DAG();
    final TestMemcacheInputOperator input = dag.addOperator("input", TestMemcacheInputOperator.class);
    CollectorModule<Object> collector = dag.addOperator("collector", new CollectorModule<Object>());

    input.addServer(server);
//    input.setRunnable();

    dag.addStream("stream",input.outputPort, collector.inputPort);

    final StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);
    long start = System.currentTimeMillis();
    new Thread("LocalClusterController")
    {
      @Override
      public void run()
      {
          while (true) {
            if (resultCount < numberOfOps) {
              try {
                Thread.sleep(100);
              }
              catch (InterruptedException ex) {
                logger.debug(ex.toString());
              }
            }
            else {
              break;
            }
          }
        lc.shutdown();
      }
    }.start();

    lc.run();
    long end = System.currentTimeMillis();
    long time = end - start;
    System.out.println("processed "+numberOfOps+" tuples in DAG used "+time+" ms or "+numberOfOps*1.0/time*1000.0+" ops");

    Assert.assertEquals("Number of emitted tuples", numberOfOps, resultList.size());
    int i=0;
    for( Object o : resultList ) {
      String str = (String)o;
      Assert.assertEquals("value of "+i+" is ", str, "Hello this is a test " + i);
      i++;
    }
    System.out.println("resultCount:"+resultCount+" i:"+i);
  }
}
