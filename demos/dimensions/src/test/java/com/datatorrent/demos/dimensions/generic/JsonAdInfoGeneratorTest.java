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
 * Tests {@link com.datatorrent.demos.dimensions.generic.JsonAdInfoGenerator}
 */
package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.demos.dimensions.ads.AdInfo;
import com.datatorrent.demos.dimensions.schemas.AdsSchemaResult;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.TestUtils;

import org.junit.Assert;

import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class JsonAdInfoGeneratorTest {

  private static final Logger LOG = LoggerFactory.getLogger(JsonAdInfoGeneratorTest.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testOperator() throws Exception
  {
    int minTuples = 1000;
    int maxPublishers = AdsSchemaResult.PUBLISHERS.length;
    int maxAdvertisers = AdsSchemaResult.ADVERTISERS.length;
    int maxAdUnits = AdsSchemaResult.LOCATIONS.length;
    JsonAdInfoGenerator oper = new JsonAdInfoGenerator();
    oper.setBlastCount(minTuples);
    CollectorTestSink<byte[]> sink = new CollectorTestSink<byte[]>();
    TestUtils.setSink(oper.jsonOutput, sink);
    oper.setup(null);
    oper.beginWindow(0);
    oper.emitTuples();
    oper.endWindow();
    oper.teardown();

    // Validate emitted tuple count
    Assert.assertTrue("Emitted tuples match blastCount", minTuples <= sink.collectedTuples.size());
    int testSize = 20;
    Random random = new Random();
    for (int i=0; i<testSize; i++) {
      // Select a JSON tuple at random for testing
      int randomIndex = random.nextInt(sink.collectedTuples.size());
      String jsonTuple =  new String(sink.collectedTuples.get(randomIndex));

      // Validate JSON structure
      Assert.assertTrue("Data in valid JSON format",  isValidJSON(jsonTuple));

      // Validate requested ranges
      AdInfo ae = mapper.readValue(jsonTuple, AdInfo.class);
      Assert.assertTrue("publisherId within range",  ae.getPublisherId() > 0 && ae.getPublisherId() <= maxPublishers);
      Assert.assertTrue("advertiserId within range",  ae.getAdvertiserId() > 0 && ae.getAdvertiserId() <= maxAdvertisers);
      Assert.assertTrue("adUnit within range",  ae.getAdUnit() > 0 && ae.getAdUnit() <= maxAdUnits);
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
