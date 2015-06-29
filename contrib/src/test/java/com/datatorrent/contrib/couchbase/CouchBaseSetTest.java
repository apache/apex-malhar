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
package com.datatorrent.contrib.couchbase;

import com.couchbase.client.CouchbaseClient;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.internal.OperationCompletionListener;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CouchBaseSetTest class which implements unit tests for Couchbase set functionality.
 */
public class CouchBaseSetTest
{
  protected static final Logger logger = LoggerFactory.getLogger(CouchBaseSetTest.class);
  public static List<Object> tuples;
  List<URI> baseURIs = new ArrayList<URI>();
  OperationFuture<Boolean> future;
  CouchbaseClient client = null;
  private final CompletionListener listener;

  public CouchBaseSetTest()
  {
    listener = new CompletionListener();
    tuples = new ArrayList<Object>();
  }

  @BeforeClass
  public static void setUpClass()
  {
  }

  @AfterClass
  public static void tearDownClass()
  {
  }

  @Before
  public void setUp()
  {

  }

  @After
  public void tearDown()
  {
  }

  @Test
  public void test()
  {
    URI uri = null;
    ArrayList<URI> nodes = new ArrayList<URI>();

    // Add one or more nodes of your cluster (exchange the IP with yours)
    nodes.add(URI.create("http://localhost:8091/pools"));

    // Try to connect to the client
    try {
      client = new CouchbaseClient(nodes, "default", "");
    }
    catch (Exception e) {
      System.err.println("Error connecting to Couchbase: " + e.getMessage());
      System.exit(1);
    }
    
    TestPojo obj = new TestPojo();
    obj.setName("test");
    obj.setPhone(123344555);
    HashMap<String, Integer> map = new HashMap<String, Integer>();
    map.put("test", 12345);
    obj.setMap(map);
    future = processKeyValue("key" , obj);
    future.addListener(listener);

  }

  long stopTime = System.currentTimeMillis();

  public OperationFuture<Boolean> processKeyValue(String key, Object value)
  {
    future = client.set(key, value);
    return future;
  }

  protected class CompletionListener implements OperationCompletionListener
  {
    @Override
    public void onComplete(OperationFuture<?> f) throws Exception
    {
      if (!((Boolean)f.get())) {
        logger.error("Operation failed " + f);
      }
      logger.info("Operation completed");
    }

  }

}
