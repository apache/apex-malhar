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
package org.apache.apex.malhar.contrib.couchbase;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.List;

import org.couchbase.mock.Bucket.BucketType;
import org.couchbase.mock.BucketConfiguration;
import org.couchbase.mock.CouchbaseMock;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.couchbase.client.CouchbaseConnectionFactory;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;

import com.datatorrent.api.Attribute.AttributeMap;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;

import com.datatorrent.netlet.util.DTThrowable;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

public class CouchBaseOutputOperatorTest
{
  private static final Logger logger = LoggerFactory.getLogger(CouchBaseOutputOperatorTest.class);
  private static final String APP_ID = "CouchBaseOutputOperatorTest";
  private static final String password = "";
  private static final int OPERATOR_ID = 0;
  protected static ArrayList<URI> nodes = new ArrayList<URI>();
  protected static ArrayList<String> keyList;
  private final int numNodes = 2;
  private final int numReplicas = 3;

  protected CouchbaseConnectionFactory connectionFactory;

  protected CouchbaseMock createMock(String name, String password, BucketConfiguration bucketConfiguration) throws Exception
  {
    bucketConfiguration.numNodes = numNodes;
    bucketConfiguration.numReplicas = numReplicas;
    bucketConfiguration.name = name;
    bucketConfiguration.type = BucketType.COUCHBASE;
    bucketConfiguration.password = password;
    bucketConfiguration.hostname = "localhost";
    ArrayList<BucketConfiguration> configList = new ArrayList<BucketConfiguration>();
    configList.add(bucketConfiguration);
    CouchbaseMock mockCouchbase = new CouchbaseMock(0, configList);
    return mockCouchbase;
  }

  public static class TestEvent1
  {

    String key;
    TestPojo test;

    public TestPojo getTest()
    {
      return test;
    }

    public void setTest(TestPojo test)
    {
      this.test = test;
    }

    public String getKey()
    {
      return key;
    }

    public void setKey(String key)
    {
      this.key = key;
    }

    TestEvent1()
    {

    }

  }

  public static class TestEvent2
  {

    String key;
    Integer num;

    public Integer getNum()
    {
      return num;
    }

    public void setNum(Integer num)
    {
      this.num = num;
    }

    public String getKey()
    {
      return key;
    }

    public void setKey(String key)
    {
      this.key = key;
    }

    TestEvent2(String key, Integer test)
    {
      this.key = key;
      this.num = test;
    }

  }

  @Test
  public void TestCouchBaseOutputOperator() throws InterruptedException, Exception
  {
    BucketConfiguration bucketConfiguration = new BucketConfiguration();
    CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
    CouchbaseMock mockCouchbase1 = createMock("default", "", bucketConfiguration);
    mockCouchbase1.start();
    mockCouchbase1.waitForStartup();

    List<URI> uriList = new ArrayList<URI>();
    int port1 = mockCouchbase1.getHttpPort();
    logger.debug("port is {}", port1);
    uriList.add(new URI("http", null, "localhost", port1, "/pools", "", ""));
    cfb.buildCouchbaseConnection(uriList, bucketConfiguration.name, bucketConfiguration.password);

    CouchBaseWindowStore store = new CouchBaseWindowStore();
    store.setBucket(bucketConfiguration.name);
    store.setPasswordConfig(password);
    store.setPassword(bucketConfiguration.password);
    store.setUriString("localhost:" + port1 + "," + "localhost:" + port1);
    try {
      store.connect();
    } catch (IOException ex) {
      DTThrowable.rethrow(ex);
    }
    store.getInstance().flush();
    store.getMetaInstance().flush();
    CouchbasePOJOSetOperator outputOperator = new CouchbasePOJOSetOperator();
    AttributeMap.DefaultAttributeMap attributeMap = new AttributeMap.DefaultAttributeMap();
    attributeMap.put(DAG.APPLICATION_ID, APP_ID);
    OperatorContext context = mockOperatorContext(OPERATOR_ID, attributeMap);

    outputOperator.setStore(store);

    outputOperator.setup(context);
    ArrayList<String> expressions = new ArrayList<String>();
    expressions.add("getKey()");
    expressions.add("getTest()");
    outputOperator.setExpressions(expressions);
    CouchBaseJSONSerializer serializer = new CouchBaseJSONSerializer();
    outputOperator.setSerializer(serializer);
    TestPojo obj = new TestPojo();
    obj.setName("test");
    obj.setPhone(123344555);
    HashMap<String, Integer> map = new HashMap<String, Integer>();
    map.put("test", 12345);
    obj.setMap(map);
    TestEvent1 testEvent = new TestEvent1();
    testEvent.setKey("key1");
    testEvent.setTest(obj);
    outputOperator.beginWindow(0);
    outputOperator.input.process(testEvent);
    outputOperator.endWindow();
    Assert.assertEquals("Value in couchbase is", "{\"name\":\"test\",\"map\":{\"test\":12345},\"phone\":123344555}", store.getInstance().get("key1").toString());
    outputOperator.teardown();
    outputOperator = new CouchbasePOJOSetOperator();
    store = new CouchBaseWindowStore();
    store.setBucket(bucketConfiguration.name);
    store.setPasswordConfig(password);
    store.setPassword(bucketConfiguration.password);
    store.setUriString("localhost:" + port1 + "," + "localhost:" + port1);
    try {
      store.connect();
    } catch (IOException ex) {
      DTThrowable.rethrow(ex);
    }
    store.getInstance().flush();
    store.getMetaInstance().flush();
    outputOperator.setStore(store);

    outputOperator.setup(context);
    expressions = new ArrayList<String>();
    expressions.add("getKey()");
    expressions.add("getNum()");
    outputOperator.setExpressions(expressions);
    TestEvent2 simpleEvent = new TestEvent2("key2", 123);
    outputOperator.beginWindow(0);
    outputOperator.input.process(simpleEvent);
    outputOperator.endWindow();
    Assert.assertEquals("Value in couchbase is", "123", store.getInstance().get("key2").toString());
    outputOperator.teardown();
    mockCouchbase1.stop();
  }

}
