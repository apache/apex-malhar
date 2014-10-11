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

/**
 * Tests {@link com.datatorrent.demos.dimensions.generic.JsonSalesGenerator}
 */
package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;
import junit.framework.Assert;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class JsonSalesGeneratorTest {

  private static final Logger LOG = LoggerFactory.getLogger(JsonSalesGeneratorTest.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testValidSettings() throws Exception
  {
    int minTuples = 1000;
    int maxProductId = 100;
    int maxCustomerId = 1000000;
    int maxChannelId = 5;
    double minAmount = 0.99;
    double maxAmount = 100.00;

    JsonSalesGenerator oper = new JsonSalesGenerator();
    oper.setMaxTuplesPerWindow(minTuples);
    oper.setTuplesPerWindowDeviation(0);
    oper.setMaxProductId(maxProductId);
    oper.setMaxCustomerId(maxCustomerId);
    oper.setMaxChannelId(maxChannelId);
    oper.setMinAmount(minAmount);
    oper.setMaxAmount(maxAmount);

    CollectorTestSink<byte[]> sink = new CollectorTestSink<byte[]>();
    TestUtils.setSink(oper.jsonBytes, sink);
    oper.setup(null);
    oper.beginWindow(0);
    oper.emitTuples();
    oper.endWindow();
    oper.teardown();

    // Validate emitted tuple count
    LOG.debug("Emitted tuples count: {}", sink.collectedTuples.size());
    Assert.assertTrue("Emitted tuples match minTuples", minTuples <= sink.collectedTuples.size());

    int testSize = 20;
    Random random = new Random();
    for (int i=0; i<testSize; i++) {
      // Select a JSON tuple at random for testing
      int randomIndex = random.nextInt(sink.collectedTuples.size());
      String jsonTuple =  new String(sink.collectedTuples.get(randomIndex));

      LOG.debug("Validating tuple: {}", jsonTuple);
      // Validate JSON structure
      Assert.assertTrue("Data in valid JSON format",  isValidJSON(jsonTuple));

      // Validate requested ranges
      SalesEvent salesEvent = mapper.readValue(jsonTuple, SalesEvent.class);
      Assert.assertTrue("customerId within range",  salesEvent.customerId > 0 && salesEvent.customerId <= maxCustomerId);
      Assert.assertTrue("productId within range",  salesEvent.productId > 0 && salesEvent.productId <= maxProductId);
      Assert.assertTrue("channelId within range",  salesEvent.channelId > 0 && salesEvent.channelId <= maxChannelId);
      Assert.assertTrue("amount within range",  salesEvent.amount >= minAmount && salesEvent.amount <= maxAmount);
      Assert.assertTrue("timestamp is valid", salesEvent.timestamp > 0 && salesEvent.timestamp <= System.currentTimeMillis());
    }
  }


  @Test
  public void testInvalidSettings() throws Exception
  {
    int minTuples = 1000;
    int maxProductId = 1;
    int maxCustomerId = -1; // Invalid value.  Should default to 1 for all customerId's
    int maxChannelId = 0; // Invalid value.  Should default to 1 for all channelId's
    double minAmount = 0.99;
    double maxAmount = 0.98; // Invalid value.  Max should be bigger than min.  Should default to 0.99 for all amounts.


    JsonSalesGenerator oper = new JsonSalesGenerator();
    oper.setMaxTuplesPerWindow(minTuples);
    oper.setMaxProductId(maxProductId);
    oper.setMaxCustomerId(maxCustomerId);
    oper.setMaxChannelId(maxChannelId);
    oper.setMinAmount(minAmount);
    oper.setMaxAmount(maxAmount);
    oper.setAddProductCategory(true);
    oper.setTuplesPerWindowDeviation(-10);


    CollectorTestSink<byte[]> sink = new CollectorTestSink<byte[]>();
    TestUtils.setSink(oper.jsonBytes, sink);
    oper.setup(null);
    oper.beginWindow(0);
    oper.emitTuples();
    oper.endWindow();
    oper.teardown();

    // Validate emitted tuple count
    LOG.debug("Emitted tuples count: {}", sink.collectedTuples.size());
    Assert.assertTrue("Emitted tuples match minTuples", minTuples <= sink.collectedTuples.size());

    int testSize = 20;
    Random random = new Random();
    for (int i=0; i<testSize; i++) {
      // Select a JSON tuple at random for testing
      int randomIndex = random.nextInt(sink.collectedTuples.size());
      String jsonTuple =  new String(sink.collectedTuples.get(randomIndex));

      LOG.debug("Validating tuple: {}", jsonTuple);
      // Validate JSON structure
      Assert.assertTrue("Data in valid JSON format",  isValidJSON(jsonTuple));

      // Validate requested ranges
      SalesEvent salesEvent = mapper.readValue(jsonTuple, SalesEvent.class);
      Assert.assertTrue("customerId within range",  salesEvent.customerId == 1);
      Assert.assertTrue("productId within range",  salesEvent.productId == maxProductId);
      Assert.assertTrue("channelId within range",  salesEvent.channelId == 1);
      Assert.assertTrue("amount within range",  salesEvent.amount == minAmount);
      Assert.assertTrue("timestamp is valid", salesEvent.timestamp > 0 && salesEvent.timestamp <= System.currentTimeMillis());
    }
  }

  public boolean isValidJSON(final String json) {
    boolean valid = false;
    try {
      final JsonParser parser = new ObjectMapper().getJsonFactory().createJsonParser(json);
      while (parser.nextToken() != null) {
        //Iterate over every available token
      }
      valid = true;
    } catch (Exception e) {
      LOG.error("Failed to parse json {}", json, e);
    }
    return valid;
  }

}
