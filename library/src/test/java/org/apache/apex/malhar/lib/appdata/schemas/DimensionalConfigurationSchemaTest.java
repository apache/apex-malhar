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
package org.apache.apex.malhar.lib.appdata.schemas;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jettison.json.JSONArray;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.appdata.schemas.DimensionalConfigurationSchema.DimensionsCombination;
import org.apache.apex.malhar.lib.appdata.schemas.DimensionalConfigurationSchema.Key;
import org.apache.apex.malhar.lib.appdata.schemas.DimensionalConfigurationSchema.Value;
import org.apache.apex.malhar.lib.dimensions.DimensionsDescriptor;
import org.apache.apex.malhar.lib.dimensions.aggregator.AggregatorIncrementalType;
import org.apache.apex.malhar.lib.dimensions.aggregator.AggregatorRegistry;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class DimensionalConfigurationSchemaTest
{
  private static final Logger logger = LoggerFactory.getLogger(DimensionalConfigurationSchemaTest.class);

  @Before
  public void initialize()
  {
    AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY.setup();
  }

  @Test
  public void noEnumTest()
  {
    //Test if loading of no enums works
    DimensionalConfigurationSchema des = new DimensionalConfigurationSchema(
        SchemaUtils.jarResourceFileToString("adsGenericEventSchemaNoEnums.json"),
        AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    DimensionalSchema dimensionalSchema = new DimensionalSchema(des);
    dimensionalSchema.getSchemaJSON();
  }

  @Test
  public void simpleTest()
  {
    final String keyName1 = "keyName1";
    final String keyName2 = "keyName2";

    final String keyName1Type = "string";
    final String keyName2Type = "string";

    final String valueName1 = "valueName1";
    final String valueName2 = "valueName2";

    final String valueName1Type = "double";
    final String valueName2Type = "integer";

    final String jsonSchema = "{\"keys\":\n" +
        "[{\"name\":\"" + keyName1 + "\",\"type\":\"" + keyName1Type + "\"},\n" +
        "{\"name\":\"" + keyName2 + "\",\"type\":\"" + keyName2Type + "\"}],\n" +
        "\"values\":\n" +
        "[{\"name\":\"" + valueName1 + "\",\"type\":\"" + valueName1Type + "\"},\n" +
        "{\"name\":\"" + valueName2 + "\",\"type\":\"" + valueName2Type + "\"}],\n" +
        "\"timeBuckets\":[all]," +
        "\"dimensions\":\n" +
        "[{\"combination\":[\"" + keyName1 + "\",\"" + keyName2 + "\"],\"additionalValues\":[\"" + valueName1 +
        ":MIN\"," + "\"" + valueName1 + ":MAX\"]},\n" +
        "{\"combination\":[\"" + keyName1 + "\"],\"additionalValues\":[\"" + valueName2 + ":SUM\"," + "\"" +
        valueName2 + ":COUNT\"]}]\n" +
        "}";

    DimensionalConfigurationSchema des = new DimensionalConfigurationSchema(jsonSchema,
        AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    FieldsDescriptor allKeysDescriptor = des.getKeyDescriptor();

    Assert.assertEquals("Incorrect number of keys.", 2, allKeysDescriptor.getFields().getFields().size());
    Assert.assertTrue("Doesn't contain required key.", allKeysDescriptor.getFields().getFields().contains(keyName1));
    Assert.assertTrue("Doesn't contain required key.", allKeysDescriptor.getFields().getFields().contains(keyName2));
    Assert.assertEquals("Key doesn't have correct type.", Type.STRING, allKeysDescriptor.getType(keyName1));
    Assert.assertEquals("Key doesn't have correct type.", Type.STRING, allKeysDescriptor.getType(keyName2));

    Assert.assertTrue("First descriptor must contain this key", des.getDimensionsDescriptorIDToKeyDescriptor().get(0).getFields().getFields().contains(keyName1));
    Assert.assertTrue("First descriptor must contain this key", des.getDimensionsDescriptorIDToKeyDescriptor().get(0).getFields().getFields().contains(keyName2));

    Assert.assertEquals("First descriptor must contain this key", Type.STRING, des.getDimensionsDescriptorIDToKeyDescriptor().get(0).getType(keyName1));
    Assert.assertEquals("First descriptor must contain this key", Type.STRING, des.getDimensionsDescriptorIDToKeyDescriptor().get(0).getType(keyName2));

    Assert.assertTrue("First descriptor must contain this key", des.getDimensionsDescriptorIDToKeyDescriptor().get(1).getFields().getFields().contains(keyName1));
    Assert.assertFalse("First descriptor must contain this key", des.getDimensionsDescriptorIDToKeyDescriptor().get(1).getFields().getFields().contains(keyName2));

    Assert.assertEquals("First descriptor must contain this key", Type.STRING, des.getDimensionsDescriptorIDToKeyDescriptor().get(1).getType(keyName1));

    //Aggregate to dimensions descriptor

    Set<String> ddKeys1 = Sets.newHashSet(keyName1, keyName2);
    Set<String> ddKeys2 = Sets.newHashSet(keyName1);

    Set<String> minAggFields = Sets.newHashSet(valueName1);
    Set<String> maxAggFields = Sets.newHashSet(valueName1);
    Set<String> sumAggFields = Sets.newHashSet(valueName2);
    Set<String> countAggFields = Sets.newHashSet(valueName2);

    logger.debug("map: {}", des.getDimensionsDescriptorIDToAggregatorToAggregateDescriptor().get(0));

    Assert.assertTrue("Incorrect aggregate fields.",
        des.getDimensionsDescriptorIDToAggregatorToAggregateDescriptor().get(0).get("MIN").getFields().getFields().equals(minAggFields));
    Assert.assertTrue("Incorrect aggregate fields.",
        des.getDimensionsDescriptorIDToAggregatorToAggregateDescriptor().get(0).get("MAX").getFields().getFields().equals(maxAggFields));
    Assert.assertTrue("Incorrect aggregate fields.",
        des.getDimensionsDescriptorIDToAggregatorToAggregateDescriptor().get(1).get("SUM").getFields().getFields().equals(sumAggFields));
    Assert.assertTrue("Incorrect aggregate fields.",
        des.getDimensionsDescriptorIDToAggregatorToAggregateDescriptor().get(1).get("COUNT").getFields().getFields().equals(countAggFields));

    final Map<String, Integer> aggToId = Maps.newHashMap();
    aggToId.put("min", 0);
    aggToId.put("max", 1);
    aggToId.put("sum", 2);
    aggToId.put("count", 3);
  }

  @Test
  public void countDescriptorTest()
  {
    final String keyName1 = "keyName1";
    final String keyName2 = "keyName2";

    final String keyName1Type = "string";
    final String keyName2Type = "string";

    final String valueName1 = "valueName1";
    final String valueName2 = "valueName2";

    final String valueName1Type = "double";
    final String valueName2Type = "integer";

    final String jsonSchema = "{\"keys\":\n" +
        "[{\"name\":\"" + keyName1 + "\",\"type\":\"" + keyName1Type + "\"},\n" +
        "{\"name\":\"" + keyName2 + "\",\"type\":\"" + keyName2Type + "\"}],\n" +
        "\"values\":\n" +
        "[{\"name\":\"" + valueName1 + "\",\"type\":\"" + valueName1Type + "\"},\n" +
        "{\"name\":\"" + valueName2 + "\",\"type\":\"" + valueName2Type + "\"}],\n" +
        "\"timeBuckets\":[\"1m\"]," +
        "\"dimensions\":\n" +
        "[{\"combination\":[\"" + keyName1 + "\",\"" + keyName2 + "\"],\"additionalValues\":[\"" + valueName1 +
        ":COUNT\"" + "]},\n" + "]\n" +
        "}";

    DimensionalConfigurationSchema des = new DimensionalConfigurationSchema(jsonSchema,
        AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    FieldsDescriptor fd = des.getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor().get(0).get(
        AggregatorIncrementalType.NAME_TO_ORDINAL.get("COUNT"));

    Assert.assertEquals("Indexes for type compress fields should be 0", 0,
        (int)fd.getTypeToFieldToIndex().get(Type.LONG).get("valueName1"));
  }

  @Test
  public void otfAggregatorDefinitionTest()
  {
    final String keyName1 = "keyName1";
    final String keyName1Type = "string";

    final String valueName1 = "valueName1";
    final String valueName1Type = "double";

    final String jsonSchema = "{\"keys\":\n" +
        "[{\"name\":\"" + keyName1 + "\",\"type\":\"" + keyName1Type + "\"}],\n" +
        "\"values\":\n" +
        "[{\"name\":\"" + valueName1 + "\",\"type\":\"" + valueName1Type + "\",\"aggregators\":[\"AVG\"]}],\n" +
        "\"timeBuckets\":[\"1m\"]," +
        "\"dimensions\":\n" +
        "[{\"combination\":[\"" + keyName1 + "\"]}]}";

    logger.debug("test schema:\n{}", jsonSchema);

    DimensionalConfigurationSchema des = new DimensionalConfigurationSchema(jsonSchema,
        AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    Assert.assertEquals(1, des.getDimensionsDescriptorIDToDimensionsDescriptor().size());

    Map<String, Type> keyFieldToType = Maps.newHashMap();
    keyFieldToType.put(keyName1, Type.STRING);
    FieldsDescriptor keyFD = new FieldsDescriptor(keyFieldToType);

    Map<String, Type> valueFieldToTypeSum = Maps.newHashMap();
    valueFieldToTypeSum.put(valueName1, Type.DOUBLE);
    FieldsDescriptor valueFDSum = new FieldsDescriptor(valueFieldToTypeSum);

    Map<String, Type> valueFieldToTypeCount = Maps.newHashMap();
    valueFieldToTypeCount.put(valueName1, Type.DOUBLE);
    FieldsDescriptor valueFDCount = new FieldsDescriptor(valueFieldToTypeCount);

    Assert.assertEquals(keyFD, des.getKeyDescriptor());
    Assert.assertEquals(valueFDSum, des.getDimensionsDescriptorIDToAggregatorToAggregateDescriptor().get(0).get("SUM"));
    Assert.assertEquals(valueFDCount, des.getDimensionsDescriptorIDToAggregatorToAggregateDescriptor().get(0).get("COUNT"));
  }

  @Test
  public void getAllKeysDescriptorTest()
  {
    DimensionalConfigurationSchema des = new DimensionalConfigurationSchema(
        SchemaUtils.jarResourceFileToString("adsGenericEventSchema.json"),
        AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    Set<String> keys = Sets.newHashSet("publisher", "advertiser", "location");

    Assert.assertEquals(keys, des.getKeyDescriptor().getFields().getFields());
  }

  @Test
  public void aggregationSchemaTest()
  {
    DimensionalConfigurationSchema des = new DimensionalConfigurationSchema(
        SchemaUtils.jarResourceFileToString("adsGenericEventSchemaAggregations.json"),
        AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    Set<String> keys = Sets.newHashSet();

    Assert.assertEquals(keys, des.getKeyDescriptor().getFields().getFields());

    Assert.assertEquals(3, des.getDimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor().size());
    Assert.assertEquals(3, des.getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor().size());
    Assert.assertEquals(3, des.getDimensionsDescriptorIDToAggregatorIDs().size());
    Assert.assertEquals(3, des.getDimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor().size());
    Assert.assertEquals(3, des.getDimensionsDescriptorIDToDimensionsDescriptor().size());
    Assert.assertEquals(3, des.getDimensionsDescriptorIDToKeyDescriptor().size());
    Assert.assertEquals(3, des.getDimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor().size());
    Assert.assertEquals(3, des.getDimensionsDescriptorIDToValueToAggregator().size());
    Assert.assertEquals(3, des.getDimensionsDescriptorIDToValueToOTFAggregator().size());
    Assert.assertEquals(1, des.getDimensionsDescriptorIDToFieldToAggregatorAdditionalValues().size());
    Assert.assertEquals(1, des.getDimensionsDescriptorIDToKeys().size());
  }

  @Test
  public void simpleOTFTest()
  {
    DimensionalConfigurationSchema des = new DimensionalConfigurationSchema(
        SchemaUtils.jarResourceFileToString("adsGenericEventSchemaOTF.json"),
        AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    Assert.assertEquals(4, des.getDimensionsDescriptorIDToAggregatorIDs().get(0).size());
  }

  @Test
  public void testConstructorAgreement()
  {
    DimensionalConfigurationSchema expectedEventSchema = new DimensionalConfigurationSchema(
        SchemaUtils.jarResourceFileToString("adsGenericEventSchemaAdditional.json"),
        AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);
    @SuppressWarnings("unchecked")
    List<Object> publisherEnumVals = (List<Object>)((List)Lists.newArrayList("twitter", "facebook", "yahoo", "google",
        "bing", "amazon"));
    @SuppressWarnings("unchecked")
    List<Object> advertiserEnumVals = (List<Object>)((List)Lists.newArrayList("starbucks", "safeway", "mcdonalds",
        "macys", "taco bell", "walmart", "khol's", "san diego zoo", "pandas", "jack in the box", "tomatina",
        "ron swanson"));
    @SuppressWarnings("unchecked")
    List<Object> locationEnumVals = (List<Object>)((List)Lists.newArrayList("N", "LREC", "SKY", "AL", "AK", "AZ", "AR",
        "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID"));

    List<Key> keys = Lists.newArrayList(new Key("publisher", Type.STRING, publisherEnumVals),
        new Key("advertiser", Type.STRING, advertiserEnumVals), new Key("location", Type.STRING, locationEnumVals));

    List<TimeBucket> timeBuckets = Lists.newArrayList(TimeBucket.MINUTE, TimeBucket.HOUR, TimeBucket.DAY);

    List<Value> values = Lists.newArrayList(new Value("impressions", Type.LONG, Sets.newHashSet("SUM", "COUNT")),
        new Value("clicks", Type.LONG, Sets.newHashSet("SUM", "COUNT")),
        new Value("cost", Type.DOUBLE, Sets.newHashSet("SUM", "COUNT")),
        new Value("revenue", Type.DOUBLE, Sets.newHashSet("SUM", "COUNT")));

    Map<String, Set<String>> valueToAggregators = Maps.newHashMap();
    valueToAggregators.put("impressions", Sets.newHashSet("MIN", "MAX"));
    valueToAggregators.put("clicks", Sets.newHashSet("MIN", "MAX"));
    valueToAggregators.put("cost", Sets.newHashSet("MIN", "MAX"));
    valueToAggregators.put("revenue", Sets.newHashSet("MIN", "MAX"));

    Set<String> emptySet = Sets.newHashSet();
    Map<String, Set<String>> emptyMap = Maps.newHashMap();

    List<DimensionsCombination> dimensionsCombinations = Lists.newArrayList(
        new DimensionsCombination(new Fields(emptySet), emptyMap),
        new DimensionsCombination(new Fields(Sets.newHashSet("location")), emptyMap),
        new DimensionsCombination(new Fields(Sets.newHashSet("advertiser")), valueToAggregators),
        new DimensionsCombination(new Fields(Sets.newHashSet("publisher")), valueToAggregators),
        new DimensionsCombination(new Fields(Sets.newHashSet("advertiser", "location")), emptyMap),
        new DimensionsCombination(new Fields(Sets.newHashSet("publisher", "location")), emptyMap),
        new DimensionsCombination(new Fields(Sets.newHashSet("publisher", "advertiser")), emptyMap),
        new DimensionsCombination(new Fields(Sets.newHashSet("publisher", "advertiser", "location")), emptyMap));

    DimensionalConfigurationSchema eventSchema = new DimensionalConfigurationSchema(keys,
        values, timeBuckets, dimensionsCombinations, AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    logger.debug("expected {}", expectedEventSchema.getDimensionsDescriptorIDToValueToOTFAggregator());
    logger.debug("actual   {}", eventSchema.getDimensionsDescriptorIDToValueToOTFAggregator());

    Assert.assertEquals(expectedEventSchema, eventSchema);
  }

  @Test
  public void testOTFAggregatorMap()
  {
    DimensionalConfigurationSchema schema = new DimensionalConfigurationSchema(
        SchemaUtils.jarResourceFileToString("adsGenericEventSchemaOTF.json"),
        AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    Set<String> otfAggregator = Sets.newHashSet("AVG");
    Set<String> valueSet = Sets.newHashSet("impressions", "clicks", "cost", "revenue");

    List<Map<String, FieldsDescriptor>> aggregatorToDescriptor = schema.getDimensionsDescriptorIDToOTFAggregatorToAggregateDescriptor();
    List<Map<String, Set<String>>> valueToAggregator = schema.getDimensionsDescriptorIDToValueToOTFAggregator();

    for (int ddId = 0; ddId < aggregatorToDescriptor.size(); ddId++) {
      Assert.assertEquals(otfAggregator, aggregatorToDescriptor.get(ddId).keySet());
      Assert.assertNotNull(aggregatorToDescriptor.get(ddId).get("AVG"));

      Assert.assertEquals(valueSet, valueToAggregator.get(ddId).keySet());
      Map<String, Set<String>> tempValueToAgg = valueToAggregator.get(ddId);

      for (Map.Entry<String, Set<String>> entry : tempValueToAgg.entrySet()) {
        Assert.assertEquals(otfAggregator, entry.getValue());
      }
    }
  }

  @Test
  public void testCustomTimeBuckets()
  {
    DimensionalConfigurationSchema schema = new DimensionalConfigurationSchema(SchemaUtils.jarResourceFileToString("adsGenericEventSchemaCustomTimeBuckets.json"), AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    Assert.assertEquals(3, schema.getTimeBuckets().size());

    Assert.assertEquals(5, schema.getCustomTimeBuckets().size());
    List<CustomTimeBucket> customTimeBuckets = Lists.newArrayList(new CustomTimeBucket(TimeBucket.MINUTE),
        new CustomTimeBucket(TimeBucket.HOUR),
        new CustomTimeBucket(TimeBucket.DAY),
        new CustomTimeBucket(TimeBucket.MINUTE, 5),
        new CustomTimeBucket(TimeBucket.HOUR, 3));
    Assert.assertEquals(customTimeBuckets, schema.getCustomTimeBuckets());

    Assert.assertEquals(40, schema.getDimensionsDescriptorIDToKeyDescriptor().size());

    JSONArray timeBucketsArray = new JSONArray();
    timeBucketsArray.put("1m").put("1h").put("1d").put("5m").put("3h");
    Assert.assertEquals(timeBucketsArray.toString(), schema.getBucketsString());

    CustomTimeBucket customTimeBucket = schema.getCustomTimeBucketRegistry().getTimeBucket(TimeBucket.MINUTE.ordinal());
    Assert.assertTrue(customTimeBucket.isUnit());
    Assert.assertEquals(TimeBucket.MINUTE, customTimeBucket.getTimeBucket());

    customTimeBucket = schema.getCustomTimeBucketRegistry().getTimeBucket(TimeBucket.HOUR.ordinal());
    Assert.assertTrue(customTimeBucket.isUnit());
    Assert.assertEquals(TimeBucket.HOUR, customTimeBucket.getTimeBucket());

    customTimeBucket = schema.getCustomTimeBucketRegistry().getTimeBucket(TimeBucket.DAY.ordinal());
    Assert.assertTrue(customTimeBucket.isUnit());
    Assert.assertEquals(TimeBucket.DAY, customTimeBucket.getTimeBucket());

    int id5m = schema.getCustomTimeBucketRegistry().getTimeBucketId(new CustomTimeBucket(TimeBucket.MINUTE, 5));
    int id3h = schema.getCustomTimeBucketRegistry().getTimeBucketId(new CustomTimeBucket(TimeBucket.HOUR, 3));

    Assert.assertEquals(256, id5m);
    Assert.assertEquals(257, id3h);

    for (int ddID = 0; ddID < schema.getDimensionsDescriptorIDToDimensionsDescriptor().size(); ddID++) {
      DimensionsDescriptor dd = schema.getDimensionsDescriptorIDToDimensionsDescriptor().get(ddID);

      Assert.assertEquals(customTimeBuckets.get(ddID % 5), dd.getCustomTimeBucket());
    }
  }

  @Test
  public void testAllCombinationsGeneration()
  {
    DimensionalConfigurationSchema schema = new DimensionalConfigurationSchema(SchemaUtils.jarResourceFileToString("adsGenericEventSimpleAllCombinations.json"), AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    Set<DimensionsDescriptor> dimensionsDescriptors = Sets.newHashSet();
    dimensionsDescriptors.add(new DimensionsDescriptor(TimeBucket.MINUTE, new Fields(new HashSet<String>())));
    dimensionsDescriptors.add(new DimensionsDescriptor(TimeBucket.MINUTE, new Fields(Sets.newHashSet("publisher"))));
    dimensionsDescriptors.add(new DimensionsDescriptor(TimeBucket.MINUTE, new Fields(Sets.newHashSet("advertiser"))));
    dimensionsDescriptors.add(new DimensionsDescriptor(TimeBucket.MINUTE, new Fields(Sets.newHashSet("location"))));
    dimensionsDescriptors.add(new DimensionsDescriptor(TimeBucket.MINUTE, new Fields(Sets.newHashSet("publisher", "advertiser"))));
    dimensionsDescriptors.add(new DimensionsDescriptor(TimeBucket.MINUTE, new Fields(Sets.newHashSet("publisher", "location"))));
    dimensionsDescriptors.add(new DimensionsDescriptor(TimeBucket.MINUTE, new Fields(Sets.newHashSet("advertiser", "location"))));
    dimensionsDescriptors.add(new DimensionsDescriptor(TimeBucket.MINUTE, new Fields(Sets.newHashSet("publisher", "advertiser", "location"))));

    Set<DimensionsDescriptor> actualDimensionsDescriptors = Sets.newHashSet();

    for (DimensionsDescriptor dimensionsDescriptor : schema.getDimensionsDescriptorToID().keySet()) {
      actualDimensionsDescriptors.add(dimensionsDescriptor);
    }

    List<DimensionsDescriptor> ddList = Lists.newArrayList(dimensionsDescriptors);
    List<DimensionsDescriptor> ddActualList = Lists.newArrayList(actualDimensionsDescriptors);

    Collections.sort(ddList);
    Collections.sort(ddActualList);

    Assert.assertEquals(dimensionsDescriptors, actualDimensionsDescriptors);
  }

  public void testLoadingSchemaWithNoTimeBucket()
  {
    DimensionalConfigurationSchema schema = new DimensionalConfigurationSchema(
        SchemaUtils.jarResourceFileToString("adsGenericEventSchemaNoTime.json"),
        AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    Assert.assertEquals(1, schema.getTimeBuckets().size());
    Assert.assertEquals(TimeBucket.ALL, schema.getTimeBuckets().get(0));
  }

  /**
   * A schema illustrating this corner case is as follows:
   * <br/>
   * <pre>
   * {@code
   * {"keys":[{"name":"publisher","type":"string"},
   *      {"name":"advertiser","type":"string"},
   *      {"name":"location","type":"string"}],
   *  "timeBuckets":["1m"],
   *  "values":
   * [{"name":"impressions","type":"long","aggregators":["SUM"]},
   *  {"name":"clicks","type":"long","aggregators":["SUM"]},
   *  {"name":"cost","type":"double","aggregators":["SUM"]},
   *  {"name":"revenue","type":"double"}],
   *  "dimensions":
   * [{"combination":[]},
   *  {"combination":["location"],"additionalValues":["revenue:SUM"]}]
   * }
   * }
   * </pre>
   */
  @Test
  public void testAdditionalValuesOneCornerCase()
  {
    DimensionalConfigurationSchema schema = new DimensionalConfigurationSchema(SchemaUtils.jarResourceFileToString("adsGenericEventSchemaAdditionalOne.json"), AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);
    int sumID = AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY.getIncrementalAggregatorNameToID().get("SUM");

    Assert.assertEquals(3, schema.getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor().get(0).get(sumID).getFieldList().size());
    Assert.assertEquals(4, schema.getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor().get(1).get(sumID).getFieldList().size());
  }

  /**
   * Tests to make sure that the correct number of dimension descriptors are produced when different time buckets are
   * specified for each dimension combination.
   */
  @Test
  public void testTimeBucketsDimensionCombination()
  {
    final int numDDIds = 11;
    final int numCombinations = 3;

    DimensionalConfigurationSchema schema = new DimensionalConfigurationSchema(SchemaUtils.jarResourceFileToString("adsGenericEventSchemaDimensionTimeBuckets.json"), AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    List<DimensionsDescriptor> dimensionsDescriptors = schema.getDimensionsDescriptorIDToDimensionsDescriptor();

    Assert.assertEquals(numDDIds, dimensionsDescriptors.size());
    Assert.assertEquals(numDDIds, schema.getDimensionsDescriptorIDToAggregatorIDToInputAggregatorDescriptor().size());
    Assert.assertEquals(numDDIds, schema.getDimensionsDescriptorIDToAggregatorIDToOutputAggregatorDescriptor().size());
    Assert.assertEquals(numDDIds, schema.getDimensionsDescriptorIDToAggregatorIDs().size());
    Assert.assertEquals(numDDIds, schema.getDimensionsDescriptorIDToAggregatorToAggregateDescriptor().size());
    Assert.assertEquals(numDDIds, schema.getDimensionsDescriptorIDToDimensionsDescriptor().size());
    Assert.assertEquals(numDDIds, schema.getDimensionsDescriptorIDToKeyDescriptor().size());
    Assert.assertEquals(numDDIds, schema.getDimensionsDescriptorIDToOTFAggregatorToAggregateDescriptor().size());
    Assert.assertEquals(numDDIds, schema.getDimensionsDescriptorIDToValueToAggregator().size());
    Assert.assertEquals(numDDIds, schema.getDimensionsDescriptorIDToValueToOTFAggregator().size());

    Assert.assertEquals(numCombinations, schema.getDimensionsDescriptorIDToFieldToAggregatorAdditionalValues().size());
    Assert.assertEquals(numCombinations, schema.getDimensionsDescriptorIDToKeys().size());

    String[] buckets = new String[]{"1m", "3d", "1h", "5s", "1m", "3d", "1m", "3d", "30s", "2h", "1d"};

    for (int ddId = 0; ddId < numDDIds; ddId++) {
      CustomTimeBucket customTimeBucket = dimensionsDescriptors.get(ddId).getCustomTimeBucket();
      Assert.assertEquals(new CustomTimeBucket(buckets[ddId]), customTimeBucket);
    }
  }

  @Test
  public void testTimeBucketsBackwardCompatibility()
  {
    DimensionalConfigurationSchema schema = new DimensionalConfigurationSchema(SchemaUtils.jarResourceFileToString("adsGenericEventSchemaDimensionTimeBuckets.json"), AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    Assert.assertEquals(2, schema.getCustomTimeBuckets().size());
  }

  private static final Logger LOG = LoggerFactory.getLogger(DimensionalConfigurationSchemaTest.class);
}
