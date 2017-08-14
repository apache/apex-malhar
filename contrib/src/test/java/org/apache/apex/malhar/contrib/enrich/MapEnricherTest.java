/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.contrib.enrich;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.io.ConsoleOutputOperator;
import org.apache.apex.malhar.lib.testbench.CollectorTestSink;
import org.apache.apex.malhar.lib.util.FieldInfo;
import org.apache.apex.malhar.lib.util.TestUtils;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;

import com.google.common.collect.Maps;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;

public class MapEnricherTest
{
  @Test
  public void includeAllKeys()
  {
    MapEnricher oper = new MapEnricher();
    oper.setStore(new MemoryStore());
    oper.setLookupFields(Arrays.asList("In1"));
    oper.setup(null);

    CollectorTestSink sink = new CollectorTestSink();
    TestUtils.setSink(oper.output, sink);

    Map<String, Object> inMap = Maps.newHashMap();
    inMap.put("In1", "Value1");
    inMap.put("In2", "Value2");

    oper.activate(null);
    oper.beginWindow(1);
    oper.input.process(inMap);
    oper.endWindow();
    oper.deactivate();

    Assert.assertEquals("includeSelectedKeys: Number of tuples emitted: ", 1, sink.collectedTuples.size());
    Assert.assertEquals("Enrich Tuple: ", "{A=Val_A, B=Val_B, C=Val_C, In2=Value2, In1=Value3}",
        sink.collectedTuples.get(0).toString());
  }

  @Test
  public void includeSelectedKeys()
  {
    MapEnricher oper = new MapEnricher();
    oper.setStore(new MemoryStore());
    oper.setLookupFields(Arrays.asList("In1"));
    oper.setIncludeFields(Arrays.asList("A", "B"));
    oper.setup(null);

    CollectorTestSink sink = new CollectorTestSink();
    TestUtils.setSink(oper.output, sink);

    Map<String, Object> inMap = Maps.newHashMap();
    inMap.put("In1", "Value1");
    inMap.put("In2", "Value2");

    oper.activate(null);
    oper.beginWindow(1);
    oper.input.process(inMap);
    oper.endWindow();
    oper.deactivate();

    Assert.assertEquals("includeSelectedKeys: Number of tuples emitted: ", 1, sink.collectedTuples.size());
    Assert.assertEquals("Enrich Tuple: ", "{A=Val_A, B=Val_B, In2=Value2, In1=Value1}",
        sink.collectedTuples.get(0).toString());
  }

  @Test
  public void testApplication() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    lma.prepareDAG(new EnrichApplication(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.run(10000);// runs for 10 seconds and quits
  }

  public static class EnrichApplication implements StreamingApplication
  {
    @Override
    public void populateDAG(DAG dag, Configuration configuration)
    {
      RandomMapGenerator input = dag.addOperator("Input", RandomMapGenerator.class);
      MapEnricher enrich = dag.addOperator("Enrich", MapEnricher.class);
      ConsoleOutputOperator console = dag.addOperator("Console", ConsoleOutputOperator.class);
      console.setSilent(true);

      List<String> includeFields = new ArrayList<>();
      includeFields.add("A");
      includeFields.add("B");
      List<String> lookupFields = new ArrayList<>();
      lookupFields.add("In1");

      enrich.setStore(new MemoryStore());
      enrich.setIncludeFields(includeFields);
      enrich.setLookupFields(lookupFields);

      dag.addStream("S1", input.output, enrich.input);
      dag.addStream("S2", enrich.output, console.input);
    }
  }

  public static class RandomMapGenerator extends BaseOperator implements InputOperator
  {
    private int key = 0;

    public final transient DefaultOutputPort output = new DefaultOutputPort();

    @Override
    public void emitTuples()
    {
      Map<String, String> map = new HashMap<>();
      map.put("In" + (key + 1), "Value" + (key + 1));
      map.put("In2", "Value3");
      output.emit(map);
    }
  }

  private static class MemoryStore implements BackendLoader
  {
    static Map<String, Map> returnData = Maps.newHashMap();
    private List<FieldInfo> includeFieldInfo;

    static {
      Map<String, String> map = Maps.newHashMap();
      map.put("A", "Val_A");
      map.put("B", "Val_B");
      map.put("C", "Val_C");
      map.put("In1", "Value3");
      returnData.put("Value1", map);

      map = Maps.newHashMap();
      map.put("A", "Val_A_1");
      map.put("B", "Val_B_1");
      map.put("C", "Val_C");
      map.put("In1", "Value3");
      returnData.put("Value2", map);
    }

    @Override
    public Map<Object, Object> loadInitialData()
    {
      return null;
    }

    @Override
    public Object get(Object key)
    {
      List<String> keyList = (List<String>)key;
      Map<String, String> keyValue = returnData.get(keyList.get(0));
      ArrayList<Object> lst = new ArrayList<Object>();

      if (CollectionUtils.isEmpty(includeFieldInfo)) {
        if (includeFieldInfo == null) {
          includeFieldInfo = new ArrayList<>();
        }
        for (Map.Entry<String, String> entry : keyValue.entrySet()) {
          // TODO: Identify the types..
          includeFieldInfo.add(new FieldInfo(entry.getKey(), entry.getKey(), FieldInfo.SupportType.OBJECT));
        }
      }

      for (FieldInfo fieldInfo : includeFieldInfo) {
        lst.add(keyValue.get(fieldInfo.getColumnName()));
      }

      return lst;
    }

    @Override
    public List<Object> getAll(List<Object> keys)
    {
      return null;
    }

    @Override
    public void put(Object key, Object value)
    {

    }

    @Override
    public void putAll(Map<Object, Object> m)
    {

    }

    @Override
    public void remove(Object key)
    {

    }

    @Override
    public void connect() throws IOException
    {

    }

    @Override
    public void disconnect() throws IOException
    {

    }

    @Override
    public boolean isConnected()
    {
      return false;
    }

    @Override
    public void setFieldInfo(List<FieldInfo> lookupFieldInfo, List<FieldInfo> includeFieldInfo)
    {
      this.includeFieldInfo = includeFieldInfo;
    }
  }
}
