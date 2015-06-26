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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;

import org.junit.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.netlet.util.DTThrowable;

/**
 * CouchBaseGetTest class which implements unit tests for Couchbase get functionality.
 */
public class CouchBaseGetTest
{
  protected static final Logger logger = LoggerFactory.getLogger(CouchBaseGetTest.class);
  public static List<Object> tuples;
  protected transient CouchbaseClient client;
  List<URI> baseURIs = new ArrayList<URI>();

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
    try {
      uri = new URI("http://localhost:8091/pools");
    }
    catch (URISyntaxException ex) {
      logger.error("Error connecting to Couchbase: " + ex.getMessage());
      DTThrowable.rethrow(ex.getCause());
    }
    baseURIs.add(uri);
    CouchbaseClient client = null;

    try {
      CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
      cfb.setOpTimeout(10000);  // wait up to 10 seconds for an operation to succeed
      cfb.setOpQueueMaxBlockTime(5000); // wait up to 5 second when trying to enqueue an operation
      client = new CouchbaseClient(cfb.buildCouchbaseConnection(baseURIs, "default", "default"));
    }
    catch (IOException ex) {
      logger.error("Error connecting to Couchbase: " + ex.getMessage());
      DTThrowable.rethrow(ex.getCause());
    }
    client.flush();
    long startTime = System.currentTimeMillis();
    logger.info("start time before get is " + startTime);

    for (int k = 0; k < 1000; k++) {
      logger.info("k " + k);

      for (int i = 0; i < 100; i++) {

        String value = client.get("Key" + (k * 100 + i)).toString();
        logger.info("value is " + value);
      }
    }
    long stopTime = System.currentTimeMillis();
    logger.info("stop time after get is " + stopTime);
    logger.info("Threads after get are + " + Thread.activeCount());

    client.shutdown();

  }

}
