/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.db;

import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.AttributeMap.DefaultAttributeMap;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAGContext;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.datamodel.converter.Converter;
import com.datatorrent.lib.db.TransactionableDataStoreWriter;
import com.datatorrent.lib.helper.OperatorContextTestHelper;

import com.datatorrent.contrib.redis.RedisStore;
import java.util.Collection;

/**
 * Test operator to test {@link TransactionableDataStoreOutputOperator}
 */
public class TransactionableDataStoreOutputOperatorTest
{
  /**
   * Test redis writer
   */
  public static class TestRedisMapWriter extends RedisStore implements TransactionableDataStoreWriter<Map<Object, Object>>
  {
    @Override
    public void process(Map<Object, Object> tuple)
    {
      putAll(tuple);
    }

    @Override
    public Map<Object, Object> retreive(Map<Object, Object> tuple)
    {
      return null;
    }

    @Override
    public void processBulk(Collection<Map<Object, Object>> tuple, long windowId)
    {

    }

    @Override
    public long retreiveLastUpdatedWindowId()
    {
      return -1;
    }

  }

  /**
   * Test {@link TransactionableDataStoreOutputOperator} using {@link TestRedisMapWriter}
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testTransactionalRedisOutput()
  {
    Converter<Map<String, Object>, Map<Object, Object>> converter = new Converter<Map<String, Object>, Map<Object, Object>>()
    {
      @Override
      public Map<Object, Object> convert(Map<String, Object> input)
      {
        return new HashMap<Object, Object>(input);
      }

    };

    TestRedisMapWriter dataStore = new TestRedisMapWriter();
    try {
      TransactionableDataStoreOutputOperator<Map<String, Object>, Map<Object, Object>> oper = new TransactionableDataStoreOutputOperator<Map<String, Object>, Map<Object, Object>>();
      oper.setStore(dataStore);
      oper.setConverter(converter);
      AttributeMap attrs = new DefaultAttributeMap();
      attrs.put(DAG.APPLICATION_ID, "test_appid");
      oper.setup(new OperatorContextTestHelper.TestIdOperatorContext(0, attrs));

      oper.beginWindow(1);

      HashMap<String, Object> map = new HashMap<String, Object>();
      map.put("t1_dim1", "dim11val");
      map.put("t1_dim2", "dim21val");
      map.put("t1_aggr1", "aggr1val");
      oper.input.process(map);

      map = new HashMap<String, Object>();
      map.put("t2_dim1", "dim12val");
      map.put("t2_aggr1", "aggr12val");
      map.put("t2_aggr2", "aggr22val");
      map.put("t2_aggr3", "aggr32val");
      oper.input.process(map);

      oper.endWindow();

      Assert.assertEquals("first tuple dimension", "dim11val", dataStore.get("t1_dim1"));
      Assert.assertEquals("first tuple aggregate", "aggr1val", dataStore.get("t1_aggr1"));
      Assert.assertEquals("second tuple dimension", "dim12val", dataStore.get("t2_dim1"));
      Assert.assertEquals("second tuple aggregate", "aggr22val", dataStore.get("t2_aggr2"));

      oper.teardown();
    }
    finally {
      dataStore.beginTransaction();
      dataStore.remove("t1_dim1");
      dataStore.remove("t1_dim2");
      dataStore.remove("t1_aggr1");
      dataStore.remove("t2_dim1");
      dataStore.remove("t2_aggr1");
      dataStore.remove("t2_aggr2");
      dataStore.remove("t2_aggr3");
      dataStore.removeCommittedWindowId("test_appid", 0);
      dataStore.commitTransaction();
    }
  }

}
