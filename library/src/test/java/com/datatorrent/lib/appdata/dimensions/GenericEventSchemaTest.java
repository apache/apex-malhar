/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.dimensions;

import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GenericEventSchemaTest
{
  private static final Logger logger = LoggerFactory.getLogger(GenericEventSchemaTest.class);

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
      "\"aggregations\":\n" +
      "[{\"descriptor\":\"" + keyName1 + ":" + keyName2 + "\",\"aggregations\":{\"" + valueName1 + "\":[\"min\",\"max\"]}},\n" +
       "{\"descriptor\":\"" + keyName1 + "\",\"aggregations\":{\"" + valueName2 + "\":[\"sum\",\"count\"]}}]\n" +
    "}";

    GenericEventSchema ges = new GenericEventSchema(jsonSchema);

    FieldsDescriptor allKeysDescriptor = ges.getAllKeysDescriptor();

    Assert.assertEquals("Incorrect number of keys.", 2, allKeysDescriptor.getFields().getFields().size());
    Assert.assertTrue("Doesn't contain required key.", allKeysDescriptor.getFields().getFields().contains(keyName1));
    Assert.assertTrue("Doesn't contain required key.", allKeysDescriptor.getFields().getFields().contains(keyName2));
    Assert.assertEquals("Key doesn't have correct type.", Type.STRING, allKeysDescriptor.getType(keyName1));
    Assert.assertEquals("Key doesn't have correct type.", Type.STRING, allKeysDescriptor.getType(keyName2));

    Assert.assertTrue("First descriptor must contain this key", ges.getDdIDToKeyDescriptor().get(0).getFields().getFields().contains(keyName1));
    Assert.assertTrue("First descriptor must contain this key", ges.getDdIDToKeyDescriptor().get(0).getFields().getFields().contains(keyName2));

    Assert.assertEquals("First descriptor must contain this key", Type.STRING, ges.getDdIDToKeyDescriptor().get(0).getType(keyName1));
    Assert.assertEquals("First descriptor must contain this key", Type.STRING, ges.getDdIDToKeyDescriptor().get(0).getType(keyName2));

    Assert.assertTrue("First descriptor must contain this key", ges.getDdIDToKeyDescriptor().get(1).getFields().getFields().contains(keyName1));
    Assert.assertFalse("First descriptor must contain this key", ges.getDdIDToKeyDescriptor().get(1).getFields().getFields().contains(keyName2));

    Assert.assertEquals("First descriptor must contain this key", Type.STRING, ges.getDdIDToKeyDescriptor().get(1).getType(keyName1));

    //Aggregate to dimensions descriptor

    Set<String> ddKeys1 = Sets.newHashSet(keyName1, keyName2);
    Set<String> ddKeys2 = Sets.newHashSet(keyName1);

    Set<String> minAggFields = Sets.newHashSet(valueName1);
    Set<String> maxAggFields = Sets.newHashSet(valueName1);
    Set<String> sumAggFields = Sets.newHashSet(valueName2);
    Set<String> countAggFields = Sets.newHashSet(valueName2);

    logger.debug("map: {}", ges.getDdIDToAggregatorToAggregateDescriptor().get(0));

    Assert.assertTrue("Incorrect aggregate fields.",
                        ges.getDdIDToAggregatorToAggregateDescriptor().get(0).get("min").getFields().getFields().equals(minAggFields));
    Assert.assertTrue("Incorrect aggregate fields.",
                        ges.getDdIDToAggregatorToAggregateDescriptor().get(0).get("max").getFields().getFields().equals(maxAggFields));
    Assert.assertTrue("Incorrect aggregate fields.",
                        ges.getDdIDToAggregatorToAggregateDescriptor().get(1).get("sum").getFields().getFields().equals(sumAggFields));
    Assert.assertTrue("Incorrect aggregate fields.",
                        ges.getDdIDToAggregatorToAggregateDescriptor().get(1).get("count").getFields().getFields().equals(countAggFields));

    final Map<String, Integer> aggToId = Maps.newHashMap();
    aggToId.put("min", 0);
    aggToId.put("max", 1);
    aggToId.put("sum", 2);
    aggToId.put("count", 3);
  }
}
