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

package com.datatorrent.demos.adsdimension.generic;

import junit.framework.Assert;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EventSchemaTest {
  private static final Logger LOG = LoggerFactory.getLogger(EventSchemaTest.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testCreateFromJSON() throws Exception {

    String sampleJSON = "{\n" +
            "  \"fields\": {\"publisherId\":\"java.lang.Integer\", \"advertiserId\":\"java.lang.Long\", \"adUnit\":\"java.lang.Integer\", \"clicks\":\"java.lang.Long\", \"price\":\"java.lang.Long\", \"cost\":\"java.lang.Double\", \"revenue\":\"java.lang.Double\", \"timestamp\":\"java.lang.Long\"},\n" +
            "  \"dimensions\": [\"time=MINUTES\", \"time=MINUTES:adUnit\", \"time=MINUTES:advertiserId\", \"time=MINUTES:publisherId\", \"time=MINUTES:advertiserId:adUnit\", \"time=MINUTES:publisherId:adUnit\", \"time=MINUTES:publisherId:advertiserId\", \"time=MINUTES:publisherId:advertiserId:adUnit\"],\n" +
            "  \"aggregates\": { \"clicks\": \"sum\", \"price\": \"sum\", \"cost\": \"sum\", \"revenue\": \"sum\"},\n" +
            "  \"timestamp\": \"timestamp\"\n" +
            "}";
    EventSchema eventSchema = EventSchema.createFromJSON(sampleJSON);

    LOG.debug("Evaluating EventSchema: {}", eventSchema.toString());

    Assert.assertNotNull("EventSchema was created", eventSchema);
    Assert.assertEquals("timestamp is timestamp", eventSchema.getTimestamp(), "timestamp");
    Assert.assertTrue("aggregates are have clicks", eventSchema.getGenericEventValues().contains("clicks") );
    Assert.assertTrue("aggregate for price is sum", eventSchema.aggregates.get("price").equals("sum") );
    Assert.assertTrue("field publisher is Integer", eventSchema.fields.get("publisherId").equals(Integer.class));
    Assert.assertTrue("field clicks is Long", eventSchema.fields.get("clicks").equals(Long.class));
    Assert.assertTrue("dimensions include time=MINUTES:publisherId:advertiserId", eventSchema.dimensions.contains("time=MINUTES:publisherId:advertiserId"));
  }


  @Test
  public void testDefaultSchemaAds() throws Exception {

    EventSchema eventSchema = EventSchema.createFromJSON(EventSchema.DEFAULT_SCHEMA_ADS);

    LOG.debug("Evaluating Ads Schema: {}", eventSchema.toString());

    Assert.assertNotNull("EventSchema was created", eventSchema);
    Assert.assertEquals("timestamp is timestamp", eventSchema.getTimestamp(), "timestamp");
    Assert.assertTrue("aggregates are have clicks", eventSchema.getGenericEventValues().contains("clicks") );
    Assert.assertTrue("aggregate for price is sum", eventSchema.aggregates.get("price").equals("sum") );
    Assert.assertTrue("field publisher is Integer", eventSchema.fields.get("publisherId").equals(Integer.class));
    Assert.assertTrue("field clicks is Long", eventSchema.fields.get("clicks").equals(Long.class));
    Assert.assertTrue("dimensions include time=MINUTES:publisherId:advertiserId", eventSchema.dimensions.contains("time=MINUTES:publisherId:advertiserId"));

  }


  @Test
  public void testDefaultSchemaSales() throws Exception {

    EventSchema eventSchema = EventSchema.createFromJSON(EventSchema.DEFAULT_SCHEMA_SALES);
    LOG.debug("Evaluating Sales Schema: {}", eventSchema.toString());

    Assert.assertNotNull("EventSchema was created", eventSchema);
    Assert.assertEquals("timestamp is timestamp", eventSchema.getTimestamp(), "timestamp");
    Assert.assertTrue("aggregates are have amount", eventSchema.getGenericEventValues().contains("amount") );
    Assert.assertTrue("aggregate for amount is sum", eventSchema.aggregates.get("amount").equals("sum") );
    Assert.assertTrue("field productId is Integer", eventSchema.fields.get("productId").equals(Integer.class));
    Assert.assertTrue("field timestamp is Long", eventSchema.fields.get("timestamp").equals(Long.class));
    Assert.assertTrue("dimensions include time=MINUTES:productCategory", eventSchema.dimensions.contains("time=MINUTES:productCategory"));
  }


}
