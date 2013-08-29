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
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
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
    public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>(){

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

    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();
    final TestMemcacheInputOperator input = dag.addOperator("input", TestMemcacheInputOperator.class);
    CollectorModule<Object> collector = dag.addOperator("collector", new CollectorModule<Object>());

    input.addServer(server);
//    input.setRunnable();

    dag.addStream("stream",input.outputPort, collector.inputPort);

    final LocalMode.Controller lc = lma.getController();
    lc.runAsync();

    long start = System.currentTimeMillis();
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
