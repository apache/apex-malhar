package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.netlet.util.Slice;
import com.google.common.collect.Maps;

import org.junit.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class GenericAggregateSerializerTest
{

  public static final String TEST_SCHEMA_JSON = "{\n" +
            "  \"fields\": {\"pubId\":\"java.lang.Integer\", \"adId\":\"java.lang.Integer\", \"adUnit\":\"java.lang.Integer\", \"clicks\":\"java.lang.Long\", \"timestamp\":\"java.lang.Long\"},\n" +
            "  \"dimensions\": [\"time=MINUTES\", \"time=MINUTES:adUnit\", \"time=MINUTES:adId\", \"time=MINUTES:pubId\", \"time=MINUTES:adId:adUnit\", \"time=MINUTES:pubId:adUnit\", \"time=MINUTES:pubId:adId\", \"time=MINUTES:pubId:adId:adUnit\"],\n" +
            "  \"aggregates\": { \"clicks\": \"sum\"},\n" +
            "  \"timestamp\": \"timestamp\"\n" +
            "}";

  /**
   * Return a EventDescrition object, to be used by operator to
   * perform aggregation, serialization and deserialization.
   * @return
   */
  public static EventSchema getEventSchema() {
    EventSchema eventSchema;
    try {
      eventSchema = EventSchema.createFromJSON(TEST_SCHEMA_JSON);
    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to parse JSON input: " + TEST_SCHEMA_JSON, e);
    }

    return eventSchema;
  }
  private static final Logger LOG = LoggerFactory.getLogger(GenericAggregateSerializerTest.class);


  @Test
  public void test()
  {
    EventSchema eventSchema = getEventSchema();
    GenericAggregateSerializer ser = new GenericAggregateSerializer(eventSchema);

    LOG.debug("eventSchema: {}", eventSchema );
    LOG.debug("keySize: {}  valLen: {} ", eventSchema.getKeyLen(), eventSchema.getValLen() );

    /* prepare an object */
    Map<String, Object> eventMap = Maps.newHashMap();
    eventMap.put("timestamp", System.currentTimeMillis());
    eventMap.put("pubId", 1);
    eventMap.put("adUnit", 2);
    eventMap.put("adId", 3);
    eventMap.put("clicks", 10L);

    GenericAggregate event = new GenericAggregate(eventSchema.convertMapToGenericEvent(eventMap));

    /* serialize and deserialize object */
    Slice keyBytes = new Slice(ser.getKey(event));
    byte[] valBytes = ser.getValue(event);

    GenericAggregate ga = ser.fromBytes(keyBytes, valBytes);

    org.junit.Assert.assertNotSame("deserialized", event, ga);

    Assert.assertEquals(ga, event);
    Assert.assertEquals("pubId", eventSchema.getValue(ga, "pubId"), eventMap.get("pubId"));
    Assert.assertEquals("adUnit", eventSchema.getValue(ga, "adUnit"), eventMap.get("adUnit"));
    Assert.assertEquals("adId", eventSchema.getValue(ga, "adId"), eventMap.get("adId"));
    Assert.assertEquals("clicks", eventSchema.getValue(ga, "clicks"), eventMap.get("clicks"));
    Assert.assertEquals("timestamp", ga.timestamp, eventMap.get("timestamp"));

    Assert.assertEquals("pubId type ", eventSchema.getClass("pubId"), Integer.class);
    Assert.assertEquals("adId type ", eventSchema.getClass("adId"), Integer.class);
    Assert.assertEquals("adUnit type ", eventSchema.getClass("adUnit"), Integer.class);
    Assert.assertEquals("click type ", eventSchema.getClass("clicks"), Long.class);
  }

  /* Test with missing fields, serialized with default values */
  @Test
  public void test1()
  {
    EventSchema eventSchema = getEventSchema();
    GenericAggregateSerializer ser = new GenericAggregateSerializer(eventSchema);

    LOG.debug("keySize: {}  valLen: {} ", eventSchema.getKeyLen(), eventSchema.getValLen() );

    /* prepare a object */
    Map<String, Object> eventMap = Maps.newHashMap();
    eventMap.put("timestamp", System.currentTimeMillis());
    eventMap.put("pubId", 1);
    eventMap.put("adUnit", 2);
    eventMap.put("clicks", 10L);

    GenericAggregate event = new GenericAggregate(eventSchema.convertMapToGenericEvent(eventMap));

    /* serialize and deserialize object */
    Slice keyBytes = new Slice(ser.getKey(event));
    byte[] valBytes = ser.getValue(event);

    GenericAggregate ga = ser.fromBytes(keyBytes, valBytes);

    Assert.assertEquals(ga, event);
    Assert.assertEquals("pubId", eventSchema.getValue(ga, "pubId"), eventMap.get("pubId"));
    Assert.assertEquals("adUnit", eventSchema.getValue(ga, "adUnit"), eventMap.get("adUnit"));
    Assert.assertEquals("adId set to null in map", eventMap.get("adId"), null);
    Assert.assertEquals("adId defaults to 0", eventSchema.getValue(ga, "adId"), 0);
    Assert.assertEquals("clicks", eventSchema.getValue(ga, "clicks"), eventMap.get("clicks"));
    Assert.assertEquals("timestamp", ga.timestamp, eventMap.get("timestamp"));

    Assert.assertEquals("pubId type ", eventSchema.getClass("pubId"), Integer.class);
    Assert.assertEquals("adId type ", eventSchema.getClass("adId"), Integer.class);
    Assert.assertEquals("adUnit type ", eventSchema.getClass("adUnit"), Integer.class);
    Assert.assertEquals("click type ", eventSchema.getClass("clicks"), Long.class);
  }

}
