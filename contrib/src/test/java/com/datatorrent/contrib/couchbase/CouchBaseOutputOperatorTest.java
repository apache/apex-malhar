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
import java.io.Serializable;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.*;
import org.python.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.helper.OperatorContextTestHelper;


import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.DAG;

import com.datatorrent.common.util.DTThrowable;

public class CouchBaseOutputOperatorTest
{

  private static final Logger logger = LoggerFactory.getLogger(CouchBaseOutputOperatorTest.class);
  private static String APP_ID = "CouchBaseOutputOperatorTest";
  private static String bucket = "default";
  private static String password = "";
  private static int OPERATOR_ID = 0;
  protected static ArrayList<URI> nodes = new ArrayList<URI>();
  protected static ArrayList<String> keyList;
  private static String uri = "127.0.0.1:8091";

  public static class TestEvent implements Serializable
  {

    String key;
    Integer value;

    TestEvent(String key, int val)
    {
      this.key = key;
      this.value = value;
    }

  }

  @Test
  public void TestCouchBaseOutputOperator()
  {
    CouchBaseWindowStore store = new CouchBaseWindowStore();
    store.setBucket(bucket);
    store.setPassword(password);
    store.setUriString(uri);
    store.setBatchSize(100);
    store.setMaxTuples(1000);
    store.setTimeout(10000);
    keyList = new ArrayList<String>();
    try {
      store.connect();
    }
    catch (IOException ex) {
      DTThrowable.rethrow(ex);
    }
    store.getInstance().flush();
    store.getMetaInstance().flush();
    CouchBaseOutputOperator outputOperator = new CouchBaseOutputOperator();
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContextTestHelper.TestIdOperatorContext context = new OperatorContextTestHelper.TestIdOperatorContext(OPERATOR_ID, attributeMap);

    outputOperator.setStore(store);

    outputOperator.setup(context);
    CouchBaseJSONSerializer serializer = new CouchBaseJSONSerializer();
    outputOperator.setSerializer(serializer);
    List<TestEvent> events = Lists.newArrayList();
    for (int i = 0; i < 1000; i++) {
      events.add(new TestEvent("key" + i, i));
      keyList.add("key" + i);
    }

    logger.info("keylist is " + keyList.toString());
    outputOperator.beginWindow(0);
    logger.info("after begin window");
    for (TestEvent event: events) {
      logger.info("before process window");
      outputOperator.generateKey(event);
      outputOperator.getValue(event);
      outputOperator.input.process(event);
      logger.info("after process window");
    }
    logger.info("before endwindow");
    outputOperator.endWindow();
    logger.info("after endwindow");
    Map<String, Object> keyValues = store.getInstance().getBulk(keyList);
    logger.info("keyValues is" + keyValues.toString());
    logger.info("size is " + keyValues.size());
    int k = outputOperator.getNumOfEventsInStore();
    Assert.assertEquals("rows in couchbase", 1000, keyValues.size());

  }

  private static class CouchBaseOutputOperator extends AbstractCouchBaseSetOperator<TestEvent>
  {

    public int getNumOfEventsInStore()
    {
      Map<String, Object> keyValues = store.client.getBulk(keyList);
      logger.info("keyValues is" + keyValues.toString());
      logger.info("size is " + keyValues.size());
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
      tuple.value = 20;
      return tuple.value;
    }

  }

}
