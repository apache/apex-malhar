/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.memcache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import net.spy.memcached.internal.OperationFuture;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class MemcacheInputOperatorTest
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(MemcacheInputOperatorTest.class);
  private ArrayList<Integer> resultList = new ArrayList<Integer>();
  private HashMap<String, Object> resultMap = new HashMap<String, Object>();

  public class TestMemcacheInputOperator extends AbstractSinglePortMemcacheInputOperator<Object>
  {
    @Override
    public Object getTuple(Entry<String, Object> e)
    {
      resultMap.put(e.getKey(), e.getValue());
      return e;
    }

    public void generateData()
    {
      HashMap<String, Integer> map = new HashMap<String, Integer>();
      map.put("a", 10);
      map.put("b", 200);
      map.put("c", 3000);
      System.out.println("Data generator map:"+map.toString());
      int exp = 60*60*24*30;
      for( Entry<String, Integer> entry : map.entrySet()) {
        System.out.println("generate key:"+entry.getKey()+" value:"+entry.getValue());
        OperationFuture<Boolean> of = client.set(entry.getKey(), exp, entry.getValue());
        try {
          if ( of.get() == false) {
            System.err.println("Set message:" + entry.getKey() + " Error!");
          }
          else {
            Object value = client.get(entry.getKey());
            System.out.println("get value:"+value);
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

  @Test
  public void testInputOperator() throws InterruptedException {
    String server = "localhost:11211";

    TestMemcacheInputOperator input = new TestMemcacheInputOperator();
    input.addKey("a");
    input.addKey("b");
    input.addKey("c");
    input.addServer(server);
    input.setup(null);

    input.generateData();
    input.activate(null);
    input.beginWindow(1);

    Thread.sleep(500);
    input.emitTuples();
    input.endWindow();
    input.deactivate();

    Assert.assertEquals("Number of emitted tuples", 3, resultMap.size());
    Assert.assertEquals("value of a is ", 10, resultMap.get("a"));
    Assert.assertEquals("value of b is ", 200, resultMap.get("b"));
    Assert.assertEquals("value of c is ", 3000, resultMap.get("c"));

  }
}
