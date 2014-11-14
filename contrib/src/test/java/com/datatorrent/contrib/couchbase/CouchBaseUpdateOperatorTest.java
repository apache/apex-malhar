/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.contrib.couchbase;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.python.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.helper.OperatorContextTestHelper;

import com.datatorrent.contrib.couchbase.CouchBaseOutputOperatorTest.TestEvent;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.DAG;

import com.datatorrent.common.util.DTThrowable;

public class CouchBaseUpdateOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(CouchBaseInputOperatorTest.class);
  private static String APP_ID = "CouchBaseInputOperatorTest";
  private static String bucket = "new";
  private static String password = "";
  private static int OPERATOR_ID = 0;
  protected static ArrayList<URI> nodes = new ArrayList<URI>();
  protected static ArrayList<String> keyList;
  private static String uri = "node13.morado.com:8091,node14.morado.com:8091";

  @Test
  public void TestCouchBaseUpdateOutputOperator()
  {
    CouchBaseWindowStore store = new CouchBaseWindowStore();

    store.setBucket(bucket);
    store.setPassword(password);
    store.setUriString(uri);
    store.setBatchSize(100);
    store.setMaxTuples(1000);
    store.setTimeout(10000);
    try {
      store.connect();
    }
    catch (IOException ex) {
      DTThrowable.rethrow(ex);
    }
    store.getInstance().flush();
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    CouchBaseUpdateOperator updateOperator = new CouchBaseUpdateOperator();
    keyList = new ArrayList<String>();
    updateOperator.setStore(store);

    updateOperator.setup(context);

    List<TestEvent> events = Lists.newArrayList();
    for (int i = 0; i < 10; i++) {
      events.add(new TestEvent("key" + i, i));
      keyList.add("key" + i);
    }

    updateOperator.beginWindow(0);
    for (TestEvent event: events) {
      updateOperator.generateKey(event);
      updateOperator.getValue(event);
      updateOperator.input.process(event);
    }
    updateOperator.endWindow();
    Assert.assertEquals("rows in couchbase", 10, updateOperator.getNumOfEventsInStore());
  }

  private static class CouchBaseUpdateOperator extends AbstractUpdateCouchBaseOutputOperator<TestEvent>
  {

    public int getNumOfEventsInStore()
    {
      Map<String, Object> keyValues = store.client.getBulk(keyList);
      logger.info("keyValues is" + keyValues.toString());
      return keyValues.size();
    }

    @Override
    public String generateKey(TestEvent tuple)
    {
      return tuple.key;
    }

    @Override
    public Object getValue(TestEvent tuple)
    {
      tuple.value = 100;
      return tuple.value;
    }


  }

}
