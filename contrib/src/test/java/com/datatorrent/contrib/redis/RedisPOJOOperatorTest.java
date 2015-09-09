/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.redis;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import redis.clients.jedis.ScanParams;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.LocalMode;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.contrib.redis.RedisInputOperatorTest.CollectorModule;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.util.FieldInfo;
import com.datatorrent.lib.util.FieldInfo.SupportType;
import com.datatorrent.lib.util.KeyValPair;

public class RedisPOJOOperatorTest
{
  private RedisStore operatorStore;
  private RedisStore testStore;

  public static class TestClass
  {
    private Integer intValue;
    private String stringValue;

    public TestClass()
    {
    }

    public TestClass(int v1, String v2)
    {
      intValue = v1;
      stringValue = v2;
    }

    public Integer getIntValue()
    {
      return intValue;
    }

    public void setIntValue(int intValue)
    {
      this.intValue = intValue;
    }

    public String getStringValue()
    {
      return stringValue;
    }

    public void setStringValue(String stringValue)
    {
      this.stringValue = stringValue;
    }
  }

  @Test
  public void testOutputOperator() throws IOException
  {
    this.operatorStore = new RedisStore();

    operatorStore.connect();
    String appId = "test_appid";
    int operatorId = 0;

    operatorStore.removeCommittedWindowId(appId, operatorId);
    operatorStore.disconnect();

    RedisPOJOOutputOperator outputOperator = new RedisPOJOOutputOperator();

    ArrayList<FieldInfo> fields = new ArrayList<FieldInfo>();

    fields.add(new FieldInfo("column1", "intValue", SupportType.INTEGER));
    fields.add(new FieldInfo("column2", "getStringValue()", SupportType.STRING));

    outputOperator.setDataColumns(fields);

    try {
      com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap attributes = new com.datatorrent.api.Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(DAG.APPLICATION_ID, appId);

      outputOperator.setStore(operatorStore);
      outputOperator.setup(new OperatorContextTestHelper.TestIdOperatorContext(operatorId, attributes));
      outputOperator.beginWindow(101);

      KeyValPair<String, Object> keyVal = new KeyValPair<String, Object>("test_abc1", new TestClass(1, "abc"));

      outputOperator.input.process(keyVal);

      outputOperator.endWindow();

      outputOperator.teardown();

      operatorStore.connect();

      Map<String, String> out = operatorStore.getMap("test_abc1");
      Assert.assertEquals("1", out.get("column1"));
      Assert.assertEquals("abc", out.get("column2"));
    } finally {
      operatorStore.remove("test_abc1");
      operatorStore.disconnect();
    }
  }

  public static class ObjectCollectorModule extends BaseOperator
  {
    volatile static Map<String, Object> resultMap = new HashMap<String, Object>();
    static long resultCount = 0;

    public final transient DefaultInputPort<KeyValPair<String, Object>> inputPort = new DefaultInputPort<KeyValPair<String, Object>>()
    {
      @Override
      public void process(KeyValPair<String, Object> tuple)
      {
        resultMap.put(tuple.getKey(), tuple.getValue());
        resultCount++;
      }
    };
  }

  @Test
  public void testInputOperator() throws IOException
  {
    @SuppressWarnings("unused")
    Class<?> clazz = org.codehaus.janino.CompilerFactory.class;

    this.operatorStore = new RedisStore();
    this.testStore = new RedisStore();

    testStore.connect();
    ScanParams params = new ScanParams();
    params.count(100);

    Map<String, String> value = new HashMap<String, String>();
    value.put("Column1", "abc");
    value.put("Column2", "1");

    Map<String, String> value1 = new HashMap<String, String>();
    value1.put("Column1", "def");
    value1.put("Column2", "2");

    Map<String, String> value2 = new HashMap<String, String>();
    value2.put("Column1", "ghi");
    value2.put("Column2", "3");

    testStore.put("test_abc_in", value);
    testStore.put("test_def_in", value1);
    testStore.put("test_ghi_in", value2);

    try {
      LocalMode lma = LocalMode.newInstance();
      DAG dag = lma.getDAG();

      RedisPOJOInputOperator inputOperator = dag.addOperator("input", new RedisPOJOInputOperator());
      final ObjectCollectorModule collector = dag.addOperator("collector", new ObjectCollectorModule());

      ArrayList<FieldInfo> fields = new ArrayList<FieldInfo>();

      fields.add(new FieldInfo("Column1", "stringValue", SupportType.STRING));
      fields.add(new FieldInfo("Column2", "intValue", SupportType.INTEGER));

      inputOperator.setDataColumns(fields);
      inputOperator.setOutputClass(TestClass.class.getName());

      inputOperator.setStore(operatorStore);
      dag.addStream("stream", inputOperator.outputPort, collector.inputPort);
      final LocalMode.Controller lc = lma.getController();

      new Thread("LocalClusterController")
      {
        @Override
        public void run()
        {
          long startTms = System.currentTimeMillis();
          long timeout = 10000L;
          try {
            Thread.sleep(1000);
            while (System.currentTimeMillis() - startTms < timeout) {
              if (ObjectCollectorModule.resultMap.size() < 3) {
                Thread.sleep(10);
              } else {
                break;
              }
            }
          } catch (InterruptedException ex) {
          }
          lc.shutdown();
        }
      }.start();

      lc.run();

      Assert.assertTrue(ObjectCollectorModule.resultMap.containsKey("test_abc_in"));
      Assert.assertTrue(ObjectCollectorModule.resultMap.containsKey("test_def_in"));
      Assert.assertTrue(ObjectCollectorModule.resultMap.containsKey("test_ghi_in"));

      TestClass a = (TestClass) ObjectCollectorModule.resultMap.get("test_abc_in");
      Assert.assertNotNull(a);
      Assert.assertEquals("abc", a.stringValue);
      Assert.assertEquals("1", a.intValue.toString());
    } finally {
      for (KeyValPair<String, String> entry : CollectorModule.resultMap) {
        testStore.remove(entry.getKey());
      }
      testStore.disconnect();
    }
  }
}
