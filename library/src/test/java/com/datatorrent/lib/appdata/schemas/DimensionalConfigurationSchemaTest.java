/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.dimensions.aggregator.AggregatorIncrementalType;
import com.datatorrent.lib.dimensions.aggregator.AggregatorUtils;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema.DimensionsCombination;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema.Key;
import com.datatorrent.lib.appdata.schemas.DimensionalConfigurationSchema.Value;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.datatorrent.lib.dimensions.aggregator.AggregatorRegistry;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class DimensionalConfigurationSchemaTest
{
  private static final Logger logger = LoggerFactory.getLogger(DimensionalConfigurationSchemaTest.class);

  @Before
  public void initialize()
  {
    AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY.setup();
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

    final String jsonSchema =
    "{\"keys\":\n" +
      "[{\"name\":\"" + keyName1 + "\",\"type\":\"" + keyName1Type + "\"},\n" +
       "{\"name\":\"" + keyName2 + "\",\"type\":\"" + keyName2Type + "\"}],\n" +
      "\"values\":\n" +
      "[{\"name\":\"" + valueName1 + "\",\"type\":\"" + valueName1Type + "\"},\n" +
       "{\"name\":\"" + valueName2 + "\",\"type\":\"" + valueName2Type + "\"}],\n" +
      "\"timeBuckets\":[all]," +
      "\"dimensions\":\n" +
      "[{\"combination\":[\"" + keyName1 + "\",\"" + keyName2 + "\"],\"additionalValues\":[\"" + valueName1 + ":MIN\"," + "\"" + valueName1 + ":MAX\"]},\n" +
       "{\"combination\":[\"" + keyName1 + "\"],\"additionalValues\":[\"" + valueName2 + ":SUM\"," + "\"" + valueName2 + ":COUNT\"]}]\n" +
    "}";

    DimensionalConfigurationSchema des = new DimensionalConfigurationSchema(jsonSchema,
                                                            AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    FieldsDescriptor allKeysDescriptor = des.getAllKeysDescriptor();

    Assert.assertEquals("Incorrect number of keys.", 2, allKeysDescriptor.getFields().getFields().size());
    Assert.assertTrue("Doesn't contain required key.", allKeysDescriptor.getFields().getFields().contains(keyName1));
    Assert.assertTrue("Doesn't contain required key.", allKeysDescriptor.getFields().getFields().contains(keyName2));
    Assert.assertEquals("Key doesn't have correct type.", Type.STRING, allKeysDescriptor.getType(keyName1));
    Assert.assertEquals("Key doesn't have correct type.", Type.STRING, allKeysDescriptor.getType(keyName2));

    Assert.assertTrue("First descriptor must contain this key", des.getDdIDToKeyDescriptor().get(0).getFields().getFields().contains(keyName1));
    Assert.assertTrue("First descriptor must contain this key", des.getDdIDToKeyDescriptor().get(0).getFields().getFields().contains(keyName2));

    Assert.assertEquals("First descriptor must contain this key", Type.STRING, des.getDdIDToKeyDescriptor().get(0).getType(keyName1));
    Assert.assertEquals("First descriptor must contain this key", Type.STRING, des.getDdIDToKeyDescriptor().get(0).getType(keyName2));

    Assert.assertTrue("First descriptor must contain this key", des.getDdIDToKeyDescriptor().get(1).getFields().getFields().contains(keyName1));
    Assert.assertFalse("First descriptor must contain this key", des.getDdIDToKeyDescriptor().get(1).getFields().getFields().contains(keyName2));

    Assert.assertEquals("First descriptor must contain this key", Type.STRING, des.getDdIDToKeyDescriptor().get(1).getType(keyName1));

    //Aggregate to dimensions descriptor

    Set<String> ddKeys1 = Sets.newHashSet(keyName1, keyName2);
    Set<String> ddKeys2 = Sets.newHashSet(keyName1);

    Set<String> minAggFields = Sets.newHashSet(valueName1);
    Set<String> maxAggFields = Sets.newHashSet(valueName1);
    Set<String> sumAggFields = Sets.newHashSet(valueName2);
    Set<String> countAggFields = Sets.newHashSet(valueName2);

    logger.debug("map: {}", des.getDdIDToAggregatorToAggregateDescriptor().get(0));

    Assert.assertTrue("Incorrect aggregate fields.",
                        des.getDdIDToAggregatorToAggregateDescriptor().get(0).get("MIN").getFields().getFields().equals(minAggFields));
    Assert.assertTrue("Incorrect aggregate fields.",
                        des.getDdIDToAggregatorToAggregateDescriptor().get(0).get("MAX").getFields().getFields().equals(maxAggFields));
    Assert.assertTrue("Incorrect aggregate fields.",
                        des.getDdIDToAggregatorToAggregateDescriptor().get(1).get("SUM").getFields().getFields().equals(sumAggFields));
    Assert.assertTrue("Incorrect aggregate fields.",
                        des.getDdIDToAggregatorToAggregateDescriptor().get(1).get("COUNT").getFields().getFields().equals(countAggFields));

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

    final String jsonSchema =
    "{\"keys\":\n" +
      "[{\"name\":\"" + keyName1 + "\",\"type\":\"" + keyName1Type + "\"},\n" +
       "{\"name\":\"" + keyName2 + "\",\"type\":\"" + keyName2Type + "\"}],\n" +
      "\"values\":\n" +
      "[{\"name\":\"" + valueName1 + "\",\"type\":\"" + valueName1Type + "\"},\n" +
       "{\"name\":\"" + valueName2 + "\",\"type\":\"" + valueName2Type + "\"}],\n" +
      "\"timeBuckets\":[\"1m\"]," +
      "\"dimensions\":\n" +
      "[{\"combination\":[\"" + keyName1 + "\",\"" + keyName2 + "\"],\"additionalValues\":[\"" + valueName1 + ":COUNT\"" + "]},\n" + "]\n" +
    "}";

    DimensionalConfigurationSchema des = new DimensionalConfigurationSchema(jsonSchema,
                                                            AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    FieldsDescriptor fd = des.getDdIDToAggIDToOutputAggDescriptor().get(0).get(AggregatorIncrementalType.NAME_TO_ORDINAL.get("COUNT"));

    Assert.assertEquals("Indexes for type compress fields should be 0", 0, (int) fd.getTypeToFieldToIndex().get(Type.LONG).get("valueName1"));
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

    Assert.assertEquals(1, des.getDdIDToDD().size());

    Map<String, Type> keyFieldToType = Maps.newHashMap();
    keyFieldToType.put(keyName1, Type.STRING);
    FieldsDescriptor keyFD = new FieldsDescriptor(keyFieldToType);

    Map<String, Type> valueFieldToTypeSum = Maps.newHashMap();
    valueFieldToTypeSum.put(valueName1, Type.DOUBLE);
    FieldsDescriptor valueFDSum = new FieldsDescriptor(valueFieldToTypeSum);

    Map<String, Type> valueFieldToTypeCount = Maps.newHashMap();
    valueFieldToTypeCount.put(valueName1, Type.DOUBLE);
    FieldsDescriptor valueFDCount = new FieldsDescriptor(valueFieldToTypeCount);

    Assert.assertEquals(keyFD, des.getAllKeysDescriptor());
    Assert.assertEquals(valueFDSum, des.getDdIDToAggregatorToAggregateDescriptor().get(0).get("SUM"));
    Assert.assertEquals(valueFDCount, des.getDdIDToAggregatorToAggregateDescriptor().get(0).get("COUNT"));
  }

  @Test
  public void getAllKeysDescriptorTest()
  {
    DimensionalConfigurationSchema des = new DimensionalConfigurationSchema(SchemaUtils.jarResourceFileToString("adsGenericEventSchema.json"),
                                                            AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    Set<String> keys = Sets.newHashSet("publisher", "advertiser", "location");

    Assert.assertEquals(keys, des.getAllKeysDescriptor().getFields().getFields());
  }

  @Test
  public void aggregationSchemaTest()
  {
    DimensionalConfigurationSchema des = new DimensionalConfigurationSchema(SchemaUtils.jarResourceFileToString("adsGenericEventSchemaAggregations.json"),
                                                            AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    Set<String> keys = Sets.newHashSet();

    Assert.assertEquals(keys, des.getAllKeysDescriptor().getFields().getFields());

    Assert.assertEquals(3, des.getDdIDToAggIDToInputAggDescriptor().size());
    Assert.assertEquals(3, des.getDdIDToAggIDToOutputAggDescriptor().size());
    Assert.assertEquals(3, des.getDdIDToAggIDs().size());
    Assert.assertEquals(3, des.getDdIDToAggregatorToAggregateDescriptor().size());
    Assert.assertEquals(3, des.getDdIDToDD().size());
    Assert.assertEquals(3, des.getDdIDToKeyDescriptor().size());
    Assert.assertEquals(3, des.getDdIDToOTFAggregatorToAggregateDescriptor().size());
    Assert.assertEquals(3, des.getDdIDToValueToAggregator().size());
    Assert.assertEquals(3, des.getDdIDToValueToOTFAggregator().size());
    Assert.assertEquals(1, des.getCombinationIDToFieldToAggregatorAdditionalValues().size());
    Assert.assertEquals(1, des.getCombinationIDToKeys().size());
  }

  @Test
  public void simpleOTFTest()
  {
    DimensionalConfigurationSchema des = new DimensionalConfigurationSchema(SchemaUtils.jarResourceFileToString("adsGenericEventSchemaOTF.json"),
                                                            AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    Assert.assertEquals(4, des.getDdIDToAggIDs().get(0).size());
  }

  @Test
  public void testConstructorAgreement()
  {
    DimensionalConfigurationSchema expectedEventSchema = new DimensionalConfigurationSchema(SchemaUtils.jarResourceFileToString("adsGenericEventSchemaAdditional.json"),
                                                                            AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);
    @SuppressWarnings("unchecked")
    List<Object> publisherEnumVals = (List<Object>) ((List) Lists.newArrayList("twitter","facebook","yahoo","google","bing","amazon"));
    @SuppressWarnings("unchecked")
    List<Object> advertiserEnumVals = (List<Object>) ((List) Lists.newArrayList("starbucks","safeway","mcdonalds","macys","taco bell","walmart","khol's","san diego zoo","pandas","jack in the box","tomatina","ron swanson"));
    @SuppressWarnings("unchecked")
    List<Object> locationEnumVals = (List<Object>) ((List) Lists.newArrayList("N","LREC","SKY","AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA","HI","ID"));

    List<Key> keys = Lists.newArrayList(new Key("publisher", Type.STRING, publisherEnumVals),
                                        new Key("advertiser", Type.STRING, advertiserEnumVals),
                                        new Key("location", Type.STRING, locationEnumVals));
    List<TimeBucket> timeBuckets = Lists.newArrayList(TimeBucket.MINUTE, TimeBucket.HOUR, TimeBucket.DAY);
    List<Value> values = Lists.newArrayList(new Value("impressions",
                                                      Type.LONG,
                                                      Sets.newHashSet("SUM", "COUNT")),
                                            new Value("clicks",
                                                      Type.LONG,
                                                      Sets.newHashSet("SUM", "COUNT")),
                                            new Value("cost",
                                                      Type.DOUBLE,
                                                      Sets.newHashSet("SUM", "COUNT")),
                                            new Value("revenue",
                                                      Type.DOUBLE,
                                                      Sets.newHashSet("SUM", "COUNT")));

    Map<String, Set<String>> valueToAggregators = Maps.newHashMap();
    valueToAggregators.put("impressions", Sets.newHashSet("MIN", "MAX"));
    valueToAggregators.put("clicks", Sets.newHashSet("MIN", "MAX"));
    valueToAggregators.put("cost", Sets.newHashSet("MIN", "MAX"));
    valueToAggregators.put("revenue", Sets.newHashSet("MIN", "MAX"));

    Set<String> emptySet = Sets.newHashSet();
    Map<String, Set<String>> emptyMap = Maps.newHashMap();

    List<DimensionsCombination> dimensionsCombinations =
    Lists.newArrayList(new DimensionsCombination(new Fields(emptySet),
                                                 emptyMap),
                       new DimensionsCombination(new Fields(Sets.newHashSet("location")),
                                                 emptyMap),
                       new DimensionsCombination(new Fields(Sets.newHashSet("advertiser")),
                                                 valueToAggregators),
                       new DimensionsCombination(new Fields(Sets.newHashSet("publisher")),
                                                 valueToAggregators),
                       new DimensionsCombination(new Fields(Sets.newHashSet("advertiser", "location")),
                                                 emptyMap),
                       new DimensionsCombination(new Fields(Sets.newHashSet("publisher", "location")),
                                                 emptyMap),
                       new DimensionsCombination(new Fields(Sets.newHashSet("publisher", "advertiser")),
                                                 emptyMap),
                       new DimensionsCombination(new Fields(Sets.newHashSet("publisher", "advertiser", "location")),
                                                 emptyMap));

    DimensionalConfigurationSchema eventSchema = new DimensionalConfigurationSchema(keys,
                                                                    values,
                                                                    timeBuckets,
                                                                    dimensionsCombinations,
                                                                    AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);

    logger.debug("expected {}", expectedEventSchema.getDdIDToValueToOTFAggregator());
    logger.debug("actual   {}", eventSchema.getDdIDToValueToOTFAggregator());

    Assert.assertEquals(expectedEventSchema, eventSchema);
  }
}
