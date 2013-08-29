/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.memcache;

import com.datatorrent.contrib.memcache.AbstractSinglePortMemcacheInputOperator;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import net.spy.memcached.internal.OperationFuture;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MemcacheInputOperatorTest
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(MemcacheInputOperatorTest.class);
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
      map.put("b", 200);
      map.put("c", 3000);
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
    public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>(){

      @Override
      public void process(T t)
      {
        HashMap<String, Object> map = (HashMap<String, Object>)t;
        resultMap.put("a", map.get("a"));
        resultMap.put("b", map.get("b"));
        resultMap.put("c", map.get("c"));
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

    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    final TestMemcacheInputOperator input = dag.addOperator("input", TestMemcacheInputOperator.class);
    CollectorModule<Object> collector = dag.addOperator("collector", new CollectorModule<Object>());

    input.addKey("a");
    input.addKey("b");
    input.addKey("c");
    input.addServer(server);

    dag.addStream("stream",input.outputPort, collector.inputPort);

    final LocalMode.Controller lc = lma.getController();
    lc.runAsync();

    try {
      while (true) {
        if (resultCount != 1) {
          Thread.sleep(10);
        }
        else {
          break;
        }
      }
    }
    catch (InterruptedException ex) {
    }
    lc.shutdown();

    Assert.assertEquals("Number of emitted tuples", 3, resultMap.size());
    Assert.assertEquals("value of a is ", 10, resultMap.get("a"));
    Assert.assertEquals("value of b is ", 200, resultMap.get("b"));
    Assert.assertEquals("value of c is ", 3000, resultMap.get("c"));
    System.out.println("resultCount:"+resultCount);
  }
}
