/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.datatorrent.lib.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;

import com.datatorrent.api.AttributeMap;
import com.datatorrent.api.DAG;

import com.datatorrent.lib.helper.OperatorContextTestHelper;

/**
 * @param <S>
 * @since 0.9.4
 */
public class TransactionableKeyValueStoreOperatorTest<S extends TransactionableKeyValueStore> extends KeyValueStoreOperatorTest<S>
{
  public TransactionableKeyValueStoreOperatorTest(S operatorStore, S testStore)
  {
    super(operatorStore, testStore);
  }

  protected static class TransactionOutputOperator<S2 extends TransactionableKeyValueStore> extends AbstractPassThruTransactionableKeyValueStoreOutputOperator<Map<String, String>, S2>
  {
    @Override
    @SuppressWarnings("unchecked")
    public void processTuple(Map<String, String> tuple)
    {
      store.putAll((Map<Object, Object>) (Map<?, ?>) tuple);
    }

  }

  public void testTransactionOutputOperator() throws IOException
  {
    TransactionableKeyValueStoreOperatorTest.TransactionOutputOperator<S> outputOperator = new TransactionableKeyValueStoreOperatorTest.TransactionOutputOperator<S>();
    String appId = "test_appid";
    int operatorId = 0;
    operatorStore.removeCommittedWindowId(appId, operatorId);

    AttributeMap.DefaultAttributeMap attributes = new AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_ID, appId);

    try {
      testStore.connect();
      outputOperator.setStore(operatorStore);
      outputOperator.setup(new OperatorContextTestHelper.TestIdOperatorContext(operatorId, attributes));
      outputOperator.beginWindow(100);
      Map<String, String> m = new HashMap<String, String>();
      m.put("test_abc", "123");
      m.put("test_def", "456");
      outputOperator.input.process(m);
      Assert.assertNull(testStore.get("test_abc"));
      Assert.assertNull(testStore.get("test_def"));
      m = new HashMap<String, String>();
      m.put("test_ghi", "789");
      outputOperator.input.process(m);
      Assert.assertNull(testStore.get("test_ghi"));
      outputOperator.endWindow();
      outputOperator.teardown();
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
