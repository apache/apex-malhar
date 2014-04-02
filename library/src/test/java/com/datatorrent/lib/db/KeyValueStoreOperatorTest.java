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
package com.datatorrent.lib.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;

import com.datatorrent.api.*;

import com.datatorrent.lib.helper.OperatorContextTestHelper;

/**
 * @param <S>
 * @since 0.9.3
 */
public class KeyValueStoreOperatorTest<S extends KeyValueStore>
{
  protected S operatorStore;
  protected S testStore;

  public KeyValueStoreOperatorTest(S operatorStore, S testStore)
  {
    this.operatorStore = operatorStore;
    this.testStore = testStore;
  }

  public static class CollectorModule<T> extends BaseOperator
  {
    static Map<String, String> resultMap = new HashMap<String, String>();
    static long resultCount = 0;
    public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>()
    {
      @Override
      public void process(T t)
      {
        @SuppressWarnings("unchecked")
        Map<String, String> map = (Map<String, String>) t;
        resultMap.putAll(map);
        resultCount++;
      }

    };
  }

  protected static class InputOperator<S2 extends KeyValueStore> extends AbstractKeyValueStoreInputOperator<Map<String, String>, S2>
  {
    @Override
    @SuppressWarnings("unchecked")
    public Map<String, String> convertToTuple(Map<Object, Object> o)
    {
      return (Map<String, String>) (Map<?, ?>) o;
    }

  }

  protected static class OutputOperator<S2 extends KeyValueStore> extends AbstractStoreOutputOperator<Map<String, String>, S2>
  {
    @Override
    @SuppressWarnings("unchecked")
    public void processTuple(Map<String, String> tuple)
    {
      store.putAll((Map<Object, Object>) (Map<?, ?>) tuple);
    }

  }

  public void testInputOperator() throws Exception
  {
    testStore.connect();
    testStore.put("test_abc", "789");
    testStore.put("test_def", "456");
    testStore.put("test_ghi", "123");
    try {
      LocalMode lma = LocalMode.newInstance();
      DAG dag = lma.getDAG();
      @SuppressWarnings("unchecked")
      InputOperator<S> inputOperator = dag.addOperator("input", new InputOperator<S>());
      CollectorModule<Object> collector = dag.addOperator("collector", new CollectorModule<Object>());
      inputOperator.addKey("test_abc");
      inputOperator.addKey("test_def");
      inputOperator.addKey("test_ghi");
      inputOperator.setStore(operatorStore);
      dag.addStream("stream", inputOperator.outputPort, collector.inputPort);
      final LocalMode.Controller lc = lma.getController();
      lc.run(3000);
      lc.shutdown();
      Assert.assertEquals("789", CollectorModule.resultMap.get("test_abc"));
      Assert.assertEquals("456", CollectorModule.resultMap.get("test_def"));
      Assert.assertEquals("123", CollectorModule.resultMap.get("test_ghi"));

    }
    finally {
      testStore.remove("test_abc");
      testStore.remove("test_def");
      testStore.remove("test_ghi");
      testStore.disconnect();
    }
  }

  public void testOutputOperator() throws IOException
  {
    OutputOperator<S> outputOperator = new OutputOperator<S>();
    try {
      AttributeMap.DefaultAttributeMap attributes = new AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.APPLICATION_ID, "test_appid");
      outputOperator.setStore(operatorStore);
      outputOperator.setup(new OperatorContextTestHelper.TestIdOperatorContext(0, attributes));
      outputOperator.beginWindow(100);
      Map<String, String> m = new HashMap<String, String>();
      m.put("test_abc", "123");
      m.put("test_def", "456");
      outputOperator.input.process(m);
      m = new HashMap<String, String>();
      m.put("test_ghi", "789");
      outputOperator.input.process(m);
      outputOperator.endWindow();
      outputOperator.teardown();
      testStore.connect();
      Assert.assertEquals("123", testStore.get("test_abc"));
      Assert.assertEquals("456", testStore.get("test_def"));
      Assert.assertEquals("789", testStore.get("test_ghi"));
    }
    finally {
      testStore.remove("test_abc");
      testStore.remove("test_def");
      testStore.remove("test_ghi");
      testStore.disconnect();
    }
  }

}
