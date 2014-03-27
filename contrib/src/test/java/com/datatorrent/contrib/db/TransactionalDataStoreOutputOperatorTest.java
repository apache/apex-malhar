/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.db;

import java.util.HashMap;
import java.util.Map;
import junit.framework.Assert;

import org.junit.Test;

import com.datatorrent.lib.datamodel.converter.Converter;

import com.datatorrent.contrib.redis.RedisMapWriter;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class TransactionalDataStoreOutputOperatorTest
{
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

    RedisMapWriter dataStore = new RedisMapWriter();
    try {
      TransactionableDataStoreOutputOperator<Map<String, Object>, Map<Object, Object>> oper = new TransactionableDataStoreOutputOperator<Map<String, Object>, Map<Object, Object>>();
      oper.setStore(dataStore);
      oper.setConverter(converter);
      oper.setup(null);

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
