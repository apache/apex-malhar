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

import java.util.*;
import junit.framework.Assert;

import org.junit.Test;

import com.datatorrent.lib.datamodel.converter.Converter;

import com.datatorrent.contrib.mongodb.MongoDBMapWriter;

/**
 * Test class to test {@link DataStoreOutputOperator}
 */
public class DataStoreOutputOperatorTest
{
  /**
   * Test the {@link DataStoreOutputOperator} using the mongoDG writer {@link MongoDBMapWriter}
   */
  @Test
  @SuppressWarnings("unchecked")
  public void testMongoDbOutput()
  {
    final String tableName = "aggregates2";
    MongoDBMapWriter<String, Object> dataStore = new MongoDBMapWriter<String, Object>();
    dataStore.setHost("localhost");
    dataStore.setDatabase("testComputations");
    dataStore.setTable(tableName);

    PassthroughConverter<Map<String, Object>> converter = new PassthroughConverter<Map<String, Object>>();
    DataStoreOutputOperator<Map<String, Object>, Map<String, Object>> oper = new DataStoreOutputOperator<Map<String, Object>, Map<String, Object>>();

    oper.setStore(dataStore);
    oper.setConverter(converter);

    oper.setup(null);
    dataStore.dropTable(tableName);

    oper.beginWindow(1);

    HashMap<String, Object> map = new HashMap<String, Object>();
    map.put("dim1", "dim11val");
    map.put("dim2", "dim21val");
    map.put("aggr1", "aggr1val");
    oper.input.process(map);

    map = new HashMap<String, Object>();
    map.put("dim1", "dim12val");
    map.put("aggr1", "aggr12val");
    map.put("aggr2", "aggr22val");
    map.put("aggr3", "aggr32val");
    oper.input.process(map);

    oper.endWindow();

    @SuppressWarnings("unchecked")
    List<Map> list = dataStore.find(tableName);

    Assert.assertEquals("result tuple count", 2, list.size());
    Map map1 = list.get(0);
    Assert.assertEquals("first tuple size", 4, map1.size());
    Assert.assertEquals("first tuple dimension", "dim11val", map1.get("dim1"));
    Assert.assertEquals("first tuple aggregate", "aggr1val", map1.get("aggr1"));
    Map map2 = list.get(1);
    Assert.assertEquals("second tuple size", 5, map2.size());
    Assert.assertEquals("second tuple dimension", "dim12val", map2.get("dim1"));
    Assert.assertEquals("second tuple aggregate", "aggr22val", map2.get("aggr2"));

    oper.teardown();

  }

  /**
   * Pass through converter which returns the same output type as input
   *
   * @param <T> type
   */
  public static class PassthroughConverter<T> implements Converter<T, T>
  {
    @Override
    public T convert(T input)
    {
      return input;
    }

  }

}
