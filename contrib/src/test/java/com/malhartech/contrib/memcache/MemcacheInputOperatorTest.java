/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.memcache;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import junit.framework.Assert;
import org.junit.Test;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class MemcacheInputOperatorTest
{
  private ArrayList<Integer> resultList = new ArrayList<Integer>();

  public class DataGenerator extends BaseOperator //implements ActivationListener<OperatorContext>
  {
    public String[] servers;
    private transient SockIOPool pool;
    protected transient MemCachedClient mcc;

    public void setServers(String[] servers) {
      this.servers = servers;
    }

    @Override
    public void setup(OperatorContext context)
    {
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

    public void generateData()
    {
      HashMap<String, Integer> map = new HashMap<String, Integer>();
      map.put("a", 10);
      map.put("b", 200);
      map.put("c", 3000);
      System.out.println("Data generator map:"+map.toString());
      for( Entry<String, Integer> entry : map.entrySet()) {
        System.out.println("generate key:"+entry.getKey()+" value:"+entry.getValue());
        if (mcc.set((String)entry.getKey(),entry.getValue()) == false) {
          System.err.println("Set message:" + entry.getKey() + " Error!");
        }
        Object value = mcc.get(entry.getKey());
        System.out.println("get value:"+value);
      }
    }

    public void setTestNum(int testNum)
    {
      //      this.testNum = testNum;
    }
  }

  public class TestMemcacheInputOperator extends AbstractSinglePortMemcacheInputOperator<Integer>
  {

    @Override
    public Integer getTuple(Object obj)
    {
      resultList.add((Integer)obj);
      return (Integer)obj;
    }

    public void generateData()
    {
      HashMap<String, Integer> map = new HashMap<String, Integer>();
      map.put("a", 10);
      map.put("b", 200);
      map.put("c", 3000);
      for( Entry<String, Integer> entry : map.entrySet()) {
        System.out.println("generate key:"+entry.getKey()+" value:"+entry.getValue());
        if (mcc.set((String)entry.getKey(),entry.getValue()) == false) {
          System.err.println("Set message:" + entry.getKey() + " Error!");
        }
        Object value = mcc.get(entry.getKey());
        System.out.println("get value:"+value);
      }
    }
  }

  @Test
  public void testInputOperator() throws InterruptedException {
//    DataGenerator generator = new DataGenerator();
    String[] servers = {"localhost:11211"};
    String[] keys = {"a", "b", "c"};
//    generator.setServers(servers);
//    generator.setup(null);
//
//    generator.generateData();

    TestMemcacheInputOperator input = new TestMemcacheInputOperator();
    input.setServers(servers);
    input.setup(null);

    input.generateData();
    input.setKeys(keys);
//
    input.activate(null);
    input.beginWindow(1);

    Thread.sleep(500);
    input.emitTuples();
    input.endWindow();
//    input.deactivate();
//
    Assert.assertEquals("Number of emitted tuples", 3, resultList.size());
    Assert.assertEquals("value of a is ", 10, resultList.get(0).intValue());
    Assert.assertEquals("value of b is ", 200, resultList.get(1).intValue());
    Assert.assertEquals("value of c is ", 3000, resultList.get(2).intValue());

  }

}
