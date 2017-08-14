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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.appdata.query.serde.MessageSerializerFactory;
import org.apache.apex.malhar.lib.dimensions.aggregator.AggregatorRegistry;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class DimensionalSchemaTest
{
  private static final String FIELD_TAGS = "tags";

  public DimensionalSchemaTest()
  {
  }

  @Before
  public void initialize()
  {
    AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY.setup();
  }

  @Test
  public void noEnumsTest()
  {
    //Test if creating schema with no enums works
    DimensionalConfigurationSchema des = new DimensionalConfigurationSchema(
        SchemaUtils.jarResourceFileToString("adsGenericEventSchemaNoEnums.json"),
        AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY);
  }

  @Test
  public void noTimeTest() throws Exception
  {
    String resultSchema = produceSchema("adsGenericEventSchemaNoTime.json");

    Map<String, String> valueToType = Maps.newHashMap();
    valueToType.put("impressions:SUM", "long");
    valueToType.put("clicks:SUM", "long");
    valueToType.put("cost:SUM", "double");
    valueToType.put("revenue:SUM", "double");

    @SuppressWarnings("unchecked")
    List<Set<String>> dimensionCombinationsList = Lists.newArrayList((Set<String>)new HashSet<String>(),
        Sets.newHashSet("location"),
        Sets.newHashSet("advertiser"),
        Sets.newHashSet("publisher"),
        Sets.newHashSet("location", "advertiser"),
        Sets.newHashSet("location", "publisher"),
        Sets.newHashSet("advertiser", "publisher"),
        Sets.newHashSet("location", "advertiser", "publisher"));

    basicSchemaChecker(resultSchema,
        Lists.newArrayList(TimeBucket.ALL.getText()),
        Lists.newArrayList("publisher", "advertiser", "location"),
        Lists.newArrayList("string", "string", "string"),
        valueToType,
        dimensionCombinationsList);
  }

  @Test
  public void globalValueTest() throws Exception
  {
    String resultSchema = produceSchema("adsGenericEventSchema.json");

    List<String> timeBuckets = Lists.newArrayList("1m", "1h", "1d");
    List<String> keyNames = Lists.newArrayList("publisher", "advertiser", "location");
    List<String> keyTypes = Lists.newArrayList("string", "string", "string");

    Map<String, String> valueToType = Maps.newHashMap();
    valueToType.put("impressions:SUM", "long");
    valueToType.put("clicks:SUM", "long");
    valueToType.put("cost:SUM", "double");
    valueToType.put("revenue:SUM", "double");

    @SuppressWarnings("unchecked")
    List<Set<String>> dimensionCombinationsList = Lists.newArrayList((Set<String>)new HashSet<String>(),
        Sets.newHashSet("location"),
        Sets.newHashSet("advertiser"),
        Sets.newHashSet("publisher"),
        Sets.newHashSet("location", "advertiser"),
        Sets.newHashSet("location", "publisher"),
        Sets.newHashSet("advertiser", "publisher"),
        Sets.newHashSet("location", "advertiser", "publisher"));

    basicSchemaChecker(resultSchema, timeBuckets, keyNames, keyTypes, valueToType, dimensionCombinationsList);
  }

  @Test
  public void additionalValueTest() throws Exception
  {
    String resultSchema = produceSchema("adsGenericEventSchemaAdditional.json");

    List<String> timeBuckets = Lists.newArrayList("1m", "1h", "1d");
    List<String> keyNames = Lists.newArrayList("publisher", "advertiser", "location");
    List<String> keyTypes = Lists.newArrayList("string", "string", "string");

    Map<String, String> valueToType = Maps.newHashMap();
    valueToType.put("impressions:SUM", "long");
    valueToType.put("impressions:COUNT", "long");
    valueToType.put("clicks:SUM", "long");
    valueToType.put("clicks:COUNT", "long");
    valueToType.put("cost:SUM", "double");
    valueToType.put("cost:COUNT", "long");
    valueToType.put("revenue:SUM", "double");
    valueToType.put("revenue:COUNT", "long");

    @SuppressWarnings("unchecked")
    List<Set<String>> dimensionCombinationsList = Lists.newArrayList((Set<String>)new HashSet<String>(),
        Sets.newHashSet("location"),
        Sets.newHashSet("advertiser"),
        Sets.newHashSet("publisher"),
        Sets.newHashSet("location", "advertiser"),
        Sets.newHashSet("location", "publisher"),
        Sets.newHashSet("advertiser", "publisher"),
        Sets.newHashSet("location", "advertiser", "publisher"));

    basicSchemaChecker(resultSchema, timeBuckets, keyNames, keyTypes, valueToType, dimensionCombinationsList);

    Map<String, String> additionalValueMap = Maps.newHashMap();
    additionalValueMap.put("impressions:MAX", "long");
    additionalValueMap.put("clicks:MAX", "long");
    additionalValueMap.put("cost:MAX", "double");
    additionalValueMap.put("revenue:MAX", "double");
    additionalValueMap.put("impressions:MIN", "long");
    additionalValueMap.put("clicks:MIN", "long");
    additionalValueMap.put("cost:MIN", "double");
    additionalValueMap.put("revenue:MIN", "double");

    @SuppressWarnings("unchecked")
    List<Map<String, String>> additionalValuesList = Lists.newArrayList(new HashMap<String, String>(),
        new HashMap<String, String>(), additionalValueMap, additionalValueMap, new HashMap<String, String>(),
        new HashMap<String, String>(), new HashMap<String, String>(), new HashMap<String, String>());

    JSONObject data = new JSONObject(resultSchema).getJSONArray("data").getJSONObject(0);
    JSONArray dimensions = data.getJSONArray("dimensions");

    for (int index = 0; index < dimensions.length(); index++) {
      JSONObject combination = dimensions.getJSONObject(index);

      Map<String, String> tempAdditionalValueMap = additionalValuesList.get(index);
      Assert.assertEquals(tempAdditionalValueMap.isEmpty(), !combination.has("additionalValues"));

      Set<String> additionalValueSet = Sets.newHashSet();

      if (tempAdditionalValueMap.isEmpty()) {
        continue;
      }

      JSONArray additionalValues = combination.getJSONArray("additionalValues");

      LOG.debug("additionalValues {}", additionalValues);

      for (int aIndex = 0; aIndex < additionalValues.length(); aIndex++) {
        JSONObject additionalValue = additionalValues.getJSONObject(aIndex);

        String valueName = additionalValue.getString("name");
        String valueType = additionalValue.getString("type");

        String expectedValueType = tempAdditionalValueMap.get(valueName);

        Assert.assertTrue("Duplicate value " + valueName, additionalValueSet.add(valueName));
        Assert.assertTrue("Invalid value " + valueName, expectedValueType != null);
        Assert.assertEquals(expectedValueType, valueType);
      }
    }
  }

  @Test
  public void enumValUpdateTest() throws Exception
  {
    String eventSchemaJSON = SchemaUtils.jarResourceFileToString("adsGenericEventSchema.json");
    DimensionalSchema dimensional = new DimensionalSchema(new DimensionalConfigurationSchema(eventSchemaJSON,
        AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY));

    Map<String, List<Object>> replacementEnums = Maps.newHashMap();
    @SuppressWarnings("unchecked")
    List<Object> publisherEnumList = ((List<Object>)((List)Lists.newArrayList("google", "twitter")));
    @SuppressWarnings("unchecked")
    List<Object> advertiserEnumList = ((List<Object>)((List)Lists.newArrayList("google", "twitter")));
    @SuppressWarnings("unchecked")
    List<Object> locationEnumList = ((List<Object>)((List)Lists.newArrayList("google", "twitter")));

    replacementEnums.put("publisher", publisherEnumList);
    replacementEnums.put("advertiser", advertiserEnumList);
    replacementEnums.put("location", locationEnumList);

    dimensional.setEnumsList(replacementEnums);

    String schemaJSON = dimensional.getSchemaJSON();

    JSONObject schema = new JSONObject(schemaJSON);
    JSONArray keys = schema.getJSONArray(DimensionalConfigurationSchema.FIELD_KEYS);

    Map<String, List<Object>> newEnums = Maps.newHashMap();

    for (int keyIndex = 0; keyIndex < keys.length(); keyIndex++) {
      JSONObject keyData = keys.getJSONObject(keyIndex);
      String name = keyData.getString(DimensionalConfigurationSchema.FIELD_KEYS_NAME);
      JSONArray enumValues = keyData.getJSONArray(DimensionalConfigurationSchema.FIELD_KEYS_ENUMVALUES);
      List<Object> enumList = Lists.newArrayList();

      for (int enumIndex = 0; enumIndex < enumValues.length(); enumIndex++) {
        enumList.add(enumValues.get(enumIndex));
      }

      newEnums.put(name, enumList);
    }

    Assert.assertEquals(replacementEnums, newEnums);
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void enumValUpdateTestComparable() throws Exception
  {
    String eventSchemaJSON = SchemaUtils.jarResourceFileToString("adsGenericEventSchema.json");
    DimensionalSchema dimensional = new DimensionalSchema(
        new DimensionalConfigurationSchema(eventSchemaJSON, AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY));

    Map<String, Set<Comparable>> replacementEnums = Maps.newHashMap();
    @SuppressWarnings("unchecked")
    Set<Comparable> publisherEnumList = ((Set<Comparable>)((Set)Sets.newHashSet("b", "c", "a")));
    @SuppressWarnings("unchecked")
    Set<Comparable> advertiserEnumList = ((Set<Comparable>)((Set)Sets.newHashSet("b", "c", "a")));
    @SuppressWarnings("unchecked")
    Set<Comparable> locationEnumList = ((Set<Comparable>)((Set)Sets.newHashSet("b", "c", "a")));

    replacementEnums.put("publisher", publisherEnumList);
    replacementEnums.put("advertiser", advertiserEnumList);
    replacementEnums.put("location", locationEnumList);

    Map<String, List<Comparable>> expectedOutput = Maps.newHashMap();
    @SuppressWarnings("unchecked")
    List<Comparable> publisherEnumSortedList = (List<Comparable>)((List)Lists.newArrayList("a", "b", "c"));
    @SuppressWarnings("unchecked")
    List<Comparable> advertiserEnumSortedList = (List<Comparable>)((List)Lists.newArrayList("a", "b", "c"));
    @SuppressWarnings("unchecked")
    List<Comparable> locationEnumSortedList = (List<Comparable>)((List)Lists.newArrayList("a", "b", "c"));

    expectedOutput.put("publisher", publisherEnumSortedList);
    expectedOutput.put("advertiser", advertiserEnumSortedList);
    expectedOutput.put("location", locationEnumSortedList);

    dimensional.setEnumsSetComparable(replacementEnums);

    String schemaJSON = dimensional.getSchemaJSON();

    JSONObject schema = new JSONObject(schemaJSON);
    JSONArray keys = schema.getJSONArray(DimensionalConfigurationSchema.FIELD_KEYS);

    Map<String, List<Comparable>> newEnums = Maps.newHashMap();

    for (int keyIndex = 0; keyIndex < keys.length(); keyIndex++) {
      JSONObject keyData = keys.getJSONObject(keyIndex);
      String name = keyData.getString(DimensionalConfigurationSchema.FIELD_KEYS_NAME);
      JSONArray enumValues = keyData.getJSONArray(DimensionalConfigurationSchema.FIELD_KEYS_ENUMVALUES);
      List<Comparable> enumList = Lists.newArrayList();

      for (int enumIndex = 0; enumIndex < enumValues.length(); enumIndex++) {
        enumList.add((Comparable)enumValues.get(enumIndex));
      }
      newEnums.put(name, enumList);
    }

    Assert.assertEquals(expectedOutput, newEnums);
  }

  @Test
  public void testSchemaTags() throws Exception
  {
    List<String> expectedTags = Lists.newArrayList("geo", "bullet");
    List<String> expectedKeyTags = Lists.newArrayList("geo.location");
    List<String> expectedValueTagsLat = Lists.newArrayList("geo.lattitude");
    List<String> expectedValueTagsLong = Lists.newArrayList("geo.longitude");

    String eventSchemaJSON = SchemaUtils.jarResourceFileToString("adsGenericEventSchemaTags.json");
    DimensionalSchema dimensional = new DimensionalSchema(
        new DimensionalConfigurationSchema(eventSchemaJSON, AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY));

    String schemaJSON = dimensional.getSchemaJSON();

    JSONObject jo = new JSONObject(schemaJSON);
    List<String> tags = getStringList(jo.getJSONArray(FIELD_TAGS));
    Assert.assertEquals(expectedTags, tags);

    JSONArray keys = jo.getJSONArray(DimensionalConfigurationSchema.FIELD_KEYS);

    List<String> keyTags = null;

    for (int keyIndex = 0; keyIndex < keys.length(); keyIndex++) {
      JSONObject key = keys.getJSONObject(keyIndex);

      if (!key.has(FIELD_TAGS)) {
        continue;
      }

      Assert.assertEquals("location", key.get(DimensionalConfigurationSchema.FIELD_KEYS_NAME));
      keyTags = getStringList(key.getJSONArray(FIELD_TAGS));
    }

    Assert.assertTrue("No tags found for any key", keyTags != null);
    Assert.assertEquals(expectedKeyTags, keyTags);

    JSONArray values = jo.getJSONArray(DimensionalConfigurationSchema.FIELD_VALUES);

    boolean valueTagsLat = false;
    boolean valueTagsLong = false;

    for (int valueIndex = 0; valueIndex < values.length(); valueIndex++) {
      JSONObject value = values.getJSONObject(valueIndex);

      if (!value.has(FIELD_TAGS)) {
        continue;
      }

      String valueName = value.getString(DimensionalConfigurationSchema.FIELD_VALUES_NAME);
      List<String> valueTags = getStringList(value.getJSONArray(FIELD_TAGS));

      LOG.debug("value name: {}", valueName);

      if (valueName.startsWith("impressions")) {
        Assert.assertEquals(expectedValueTagsLat, valueTags);
        valueTagsLat = true;
      } else if (valueName.startsWith("clicks")) {
        Assert.assertEquals(expectedValueTagsLong, valueTags);
        valueTagsLong = true;
      } else {
        Assert.fail("There should be no tags for " + valueName);
      }
    }

    Assert.assertTrue("No tags found for impressions", valueTagsLat);
    Assert.assertTrue("No tags found for clicks", valueTagsLong);
  }

  private String produceSchema(String resourceName) throws Exception
  {
    String eventSchemaJSON = SchemaUtils.jarResourceFileToString(resourceName);

    MessageSerializerFactory dsf = new MessageSerializerFactory(new ResultFormatter());
    DimensionalSchema schemaDimensional = new DimensionalSchema(
        new DimensionalConfigurationSchema(eventSchemaJSON, AggregatorRegistry.DEFAULT_AGGREGATOR_REGISTRY));

    SchemaQuery schemaQuery = new SchemaQuery("1");

    SchemaResult result = new SchemaResult(schemaQuery, schemaDimensional);
    return dsf.serialize(result);
  }

  private List<String> getStringList(JSONArray ja) throws Exception
  {
    List<String> stringsArray = Lists.newArrayList();

    for (int index = 0; index < ja.length(); index++) {
      stringsArray.add(ja.getString(index));
    }

    return stringsArray;
  }

  private void basicSchemaChecker(String resultSchema, List<String> timeBuckets, List<String> keyNames,
      List<String> keyTypes, Map<String, String> valueToType, List<Set<String>> dimensionCombinationsList)
      throws Exception
  {
    LOG.debug("Schema to check {}", resultSchema);
    JSONObject schemaJO = new JSONObject(resultSchema);
    JSONObject data = schemaJO.getJSONArray("data").getJSONObject(0);

    JSONArray jaBuckets = SchemaUtils.findFirstKeyJSONArray(schemaJO, "buckets");

    Assert.assertEquals(timeBuckets.size(), jaBuckets.length());

    for (int index = 0; index < jaBuckets.length(); index++) {
      Assert.assertEquals(timeBuckets.get(index), jaBuckets.get(index));
    }

    JSONArray keys = data.getJSONArray("keys");

    for (int index = 0; index < keys.length(); index++) {
      JSONObject keyJO = keys.getJSONObject(index);

      Assert.assertEquals(keyNames.get(index), keyJO.get("name"));
      Assert.assertEquals(keyTypes.get(index), keyJO.get("type"));
      Assert.assertTrue(keyJO.has("enumValues"));
    }

    JSONArray valuesArray = data.getJSONArray("values");

    Assert.assertEquals("Incorrect number of values.", valueToType.size(), valuesArray.length());

    Set<String> valueNames = Sets.newHashSet();

    for (int index = 0; index < valuesArray.length(); index++) {
      JSONObject valueJO = valuesArray.getJSONObject(index);

      String valueName = valueJO.getString("name");
      String typeName = valueJO.getString("type");

      String expectedType = valueToType.get(valueName);

      Assert.assertTrue("Duplicate value name " + valueName, valueNames.add(valueName));
      Assert.assertTrue("Invalid value name " + valueName, expectedType != null);

      Assert.assertEquals(expectedType, typeName);
    }

    JSONArray dimensions = data.getJSONArray("dimensions");

    for (int index = 0; index < dimensions.length(); index++) {
      JSONObject combination = dimensions.getJSONObject(index);
      JSONArray dimensionsCombinationArray = combination.getJSONArray("combination");

      Set<String> dimensionCombination = Sets.newHashSet();

      for (int dimensionIndex = 0; dimensionIndex < dimensionsCombinationArray.length(); dimensionIndex++) {
        dimensionCombination.add(dimensionsCombinationArray.getString(dimensionIndex));
      }

      Assert.assertEquals(dimensionCombinationsList.get(index), dimensionCombination);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(DimensionalSchemaTest.class);
}
