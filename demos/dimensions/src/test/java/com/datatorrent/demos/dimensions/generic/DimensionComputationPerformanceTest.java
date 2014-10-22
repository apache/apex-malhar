package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.common.util.Slice;
import com.datatorrent.demos.dimensions.ads.AdInfo;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class DimensionComputationPerformanceTest
{
  private static final Logger LOG = LoggerFactory.getLogger(DimensionComputationPerformanceTest.class);

  public Random random = new Random();

  public AdInfo generateAdEvent()
  {
    AdInfo info = new AdInfo();
    info.setTimestamp(System.currentTimeMillis());
    info.setAdUnit(random.nextInt(10));
    info.setPublisherId(random.nextInt(10));
    info.setAdvertiserId(random.nextInt(10));
    info.setClicks(1);
    return info;
  }

  long timeDimensions(int count)
  {
    AdInfo info;
    String TEST_SCHEMA_JSON = "{\n" +
        "  \"fields\": {\"pubId\":\"java.lang.Integer\", \"adId\":\"java.lang.Integer\", \"adUnit\":\"java.lang.Integer\", \"clicks\":\"java.lang.Long\", \"timestamp\":\"java.lang.Long\"},\n" +
        "  \"dimensions\": [\"time=MINUTES\", \"time=MINUTES:adUnit\", \"time=MINUTES:adId\", \"time=MINUTES:pubId\", \"time=MINUTES:adId:adUnit\", \"time=MINUTES:pubId:adUnit\", \"time=MINUTES:pubId:adId\", \"time=MINUTES:pubId:adId:adUnit\"],\n" +
        "  \"aggregates\": { \"clicks\": \"sum\"},\n" +
        "  \"timestamp\": \"timestamp\"\n" +
        "}";

    GenericDimensionComputation dimensions = new GenericDimensionComputation();
    dimensions.setEventSchemaJSON(TEST_SCHEMA_JSON);
    dimensions.setup(null);

    long start_time = System.currentTimeMillis();
    for(int i = 0; i < count; i++)
    {
      info = generateAdEvent();
      Map<String, Object> tuple  = Maps.newHashMap();
      tuple.put("pubId", info.getPublisherId());
      tuple.put("adId", info.getAdvertiserId());
      tuple.put("adUnit", info.getAdUnit());
      tuple.put("clicks", info.getClicks());
      tuple.put("timestamp", info.getTimestamp());

      dimensions.data.process(tuple);
    }
    return System.currentTimeMillis() - start_time;
  }

  long timeDimensionsWithKryo(int count) throws InterruptedException
  {
    AdInfo info;
    String TEST_SCHEMA_JSON = "{\n" +
        "  \"fields\": {\"pubId\":\"java.lang.Integer\", \"adId\":\"java.lang.Integer\", \"adUnit\":\"java.lang.Integer\", \"clicks\":\"java.lang.Long\", \"timestamp\":\"java.lang.Long\"},\n" +
        "  \"dimensions\": [\"time=MINUTES\", \"time=MINUTES:adUnit\", \"time=MINUTES:adId\", \"time=MINUTES:pubId\", \"time=MINUTES:adId:adUnit\", \"time=MINUTES:pubId:adUnit\", \"time=MINUTES:pubId:adId\", \"time=MINUTES:pubId:adId:adUnit\"],\n" +
        "  \"aggregates\": { \"clicks\": \"sum\"},\n" +
        "  \"timestamp\": \"timestamp\"\n" +
        "}";

    GenericDimensionComputation dimensions = new GenericDimensionComputation();
    dimensions.setEventSchemaJSON(TEST_SCHEMA_JSON);
    dimensions.setup(null);
    KryoSerializableStreamCodec codec = new KryoSerializableStreamCodec();
    codec.register(HashMap.class);
    long start_time = System.currentTimeMillis();
    for(int i = 0; i < count; i++)
    {
      info = generateAdEvent();
      Map<String, Object> tuple  = Maps.newHashMap();
      tuple.put("pubId", info.getPublisherId());
      tuple.put("adId", info.getAdvertiserId());
      tuple.put("adUnit", info.getAdUnit());
      tuple.put("clicks", info.getClicks());
      tuple.put("timestamp", info.getTimestamp());

      Slice slice = codec.toByteArray(tuple);
      Object o = codec.fromByteArray(slice);

      dimensions.data.process((Map<String, Object>) o);
    }
    return System.currentTimeMillis() - start_time;
  }

  @Test @Ignore
  public void test() throws InterruptedException, IOException
  {
    int tuplesCount = 1000000;
    long dimMS = timeDimensions(tuplesCount);
    LOG.debug("Dimensions computation for {} tuples: {} ms", tuplesCount, dimMS);
    Assert.assertTrue("Dimensions sufficiently fast", dimMS < 10000);
    long dimWithKryoMS = timeDimensionsWithKryo(tuplesCount);
    LOG.debug("Dimensions computation with Kryo for {} tuples: {} ms", tuplesCount, dimWithKryoMS);
    Assert.assertTrue("Dimensions with Kryo sufficiently fast", dimMS < 20000);
  }
}
