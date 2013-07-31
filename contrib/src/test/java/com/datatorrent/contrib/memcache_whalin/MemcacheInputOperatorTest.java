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
package com.datatorrent.contrib.memcache_whalin;

import com.datatorrent.contrib.memcache_whalin.AbstractSinglePortMemcacheInputOperator;
import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.whalin.MemCached.MemCachedClient;
import com.whalin.MemCached.SockIOPool;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map.Entry;
import junit.framework.Assert;
import org.junit.Test;

/**
 *
 * @author Zhongjian Wang <zhongjian@datatorrent.com>
 */
public class MemcacheInputOperatorTest
{
  private ArrayList<Integer> resultList = new ArrayList<Integer>();

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
