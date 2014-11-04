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

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.datatorrent.common.util.DTThrowable;
import java.util.concurrent.atomic.AtomicInteger;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;
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

  public CouchBaseSetTest()
  {

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
    nodes.add(URI.create("http://127.0.0.1:8091/pools"));

    // Try to connect to the client
    CouchbaseClient client = null;
    try {
      client = new CouchbaseClient(nodes, "default", "");
    } catch (Exception e) {
      System.err.println("Error connecting to Couchbase: " + e.getMessage());
      System.exit(1);
    }
      /*for (int i = 0; i < 100; i++) {

        final OperationFuture<Boolean> future = client.set("Key" + (k * 100 + i), i);
        future.addListener(new OperationCompletionListener()
        {

          @Override
          public void onComplete(OperationFuture<?> f) throws Exception
          {
            countLatch.countDown();
            if (!((Boolean)f.get())) {
              logger.info("Noway");
            }
            j.incrementAndGet();

          }

        });
      }
      try {
        countLatch.await();
      }
      catch (InterruptedException ex) {
        logger.error("Error connecting to Couchbase: " + ex.getMessage());
        DTThrowable.rethrow(ex.getCause());
      }*/
      TestPojo obj = new TestPojo();
      obj.setName("prerna");
      obj.setPhone(123344555);
      HashMap<String,Integer> map = new HashMap<String, Integer>();
      map.put("prer", 12345);
      client.set("key", obj);
    }
    long stopTime = System.currentTimeMillis();


 

}
