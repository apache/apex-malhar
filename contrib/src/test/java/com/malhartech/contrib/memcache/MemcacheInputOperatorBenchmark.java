/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.memcache;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DAG;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.stram.StramLocalCluster;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import net.spy.memcached.internal.OperationFuture;
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
  private static HashMap<String, Object> resultMap = new HashMap<String, Object>();
  private static int resultCount=0;

  public static class TestMemcacheInputOperator extends AbstractSinglePortMemcacheInputOperator<Object>
  {
    @Override
    public Object getTuple(Object o)
    {
      return o;
    }

    public void generateData()
    {
      HashMap<String, Integer> map = new HashMap<String, Integer>();
      map.put("a", 10);
//      map.put("b", 200);
//      map.put("c", 3000);
      System.out.println("Data generator map:"+map.toString());
      int exp = 60*60*24*30;
      for( Entry<String, Integer> entry : map.entrySet()) {
        OperationFuture<Boolean> of = client.set(entry.getKey(), exp, entry.getValue());
        try {
          if ( of.get() == false) {
            System.err.println("Set message:" + entry.getKey() + " Error!");
          }
        }
        catch (InterruptedException ex) {
          logger.debug(ex.toString());
        }
        catch (ExecutionException ex) {
          logger.debug(ex.toString());
        }
      }
    }
  }

  public static class CollectorModule<T> extends BaseOperator
  {
    public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>(this){

      @Override
      public void process(T t)
      {
        HashMap<String, Object> map = (HashMap<String, Object>)t;
        resultMap.put("a", map.get("a"));
//        resultMap.put("b", map.get("b"));
//        resultMap.put("c", map.get("c"));
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

    DAG dag = new DAG();
    final TestMemcacheInputOperator input = dag.addOperator("input", TestMemcacheInputOperator.class);
    CollectorModule<Object> collector = dag.addOperator("collector", new CollectorModule<Object>());

    final int readNum = 10000;
    input.addKey("a");
//    input.addKey("b");
//    input.addKey("c");
    input.addServer(server);
    input.setReadNum(readNum);

    dag.addStream("stream",input.outputPort, collector.inputPort);

    final StramLocalCluster lc = new StramLocalCluster(dag);
    lc.setHeartbeatMonitoringEnabled(false);

    new Thread("LocalClusterController")
    {
      @Override
      public void run()
      {
        try {

          while (true) {
            if (resultCount < readNum) {
              Thread.sleep(10);
//              if( resultCount % 1000 == 0)
//                logger.debug("processed "+resultCount+" tuples");
            }
            else {
              break;
            }
          }
        }
        catch (InterruptedException ex) {
        }
        lc.shutdown();
      }
    }.start();

    lc.run();


    Assert.assertEquals("Number of emitted tuples", 1, resultMap.size());
    Assert.assertEquals("value of a is ", 10, resultMap.get("a"));
//    Assert.assertEquals("value of b is ", 200, resultMap.get("b"));
//    Assert.assertEquals("value of c is ", 3000, resultMap.get("c"));
    System.out.println("resultCount:"+resultCount);
  }
}
