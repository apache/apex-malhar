package com.datatorrent.demos.adsdimension.generic;

import com.datatorrent.common.util.Slice;
import com.datatorrent.demos.adsdimension.AdInfo;
import com.datatorrent.lib.codec.KryoSerializableStreamCodec;
import com.google.common.collect.Maps;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class DimensionComputationBenchmark
{
  public static Random random = new Random();

  public static AdInfo generateAdEvent()
  {
    AdInfo info = new AdInfo();
    info.setTimestamp(System.currentTimeMillis());
    info.setAdUnit(random.nextInt(10));
    info.setPublisherId(random.nextInt(10));
    info.setAdvertiserId(random.nextInt(10));
    info.setClicks(1);
    return info;
  }

  static void test1(int count)
  {
    AdInfo info;
    ObjectMapper mapper = new ObjectMapper();
    ObjectReader reader = mapper.reader(new TypeReference<Map<String,Object>>() { });
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
      tuple.put("pubId", new Integer(info.getPublisherId()));
      tuple.put("adId", new Integer(info.getAdvertiserId()));
      tuple.put("adUnit", new Integer(info.getAdUnit()));
      tuple.put("clicks", new Long(info.getClicks()));
      tuple.put("timestamp", new Long(info.getTimestamp()));

      dimensions.data.process(tuple);
    }
    System.out.println("Total time taken " + (System.currentTimeMillis() - start_time));
  }


  static void test3(int count)
  {
    AdInfo info;
    ObjectMapper mapper = new ObjectMapper();
    ObjectReader reader = mapper.reader(new TypeReference<Map<String,Object>>() { });
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
      tuple.put("pubId", new Integer(info.getPublisherId()));
      tuple.put("adId", new Integer(info.getAdvertiserId()));
      tuple.put("adUnit", new Integer(info.getAdUnit()));
      tuple.put("clicks", new Long(info.getClicks()));
      tuple.put("timestamp", new Long(info.getTimestamp()));

      dimensions.data.process(tuple);
    }
    System.out.println("Total time taken " + (System.currentTimeMillis() - start_time));
  }

  public static void test2(int count) throws InterruptedException
  {
    AdInfo info;
    ObjectMapper mapper = new ObjectMapper();
    ObjectReader reader = mapper.reader(new TypeReference<Map<String,Object>>() { });
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
      tuple.put("pubId", new Integer(info.getPublisherId()));
      tuple.put("adId", new Integer(info.getAdvertiserId()));
      tuple.put("adUnit", new Integer(info.getAdUnit()));
      tuple.put("clicks", new Long(info.getClicks()));
      tuple.put("timestamp", new Long(info.getTimestamp()));

      Slice slice = codec.toByteArray(tuple);
      Object o = codec.fromByteArray(slice);

      dimensions.data.process((Map<String, Object>) o);
    }
    System.out.println("Total time taken " + (System.currentTimeMillis() - start_time));
  }

  public static void main(String[] args) throws InterruptedException, IOException
  {
    test1(1000000);
    test2(1000000);
    test3(1000000);
  }
}
