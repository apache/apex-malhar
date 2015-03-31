/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GenericSchemaTabularTest
{
  @Test
  public void schemaTabularTest()
  {
    String schemaJSON = "{\n"
                        + " \"schemaType\": \"twitterTop10\",\n"
                        + " \"schemaVersion\": \"1.0\",\n"
                        + " \"values\": [\n"
                        + "   {\"name\": \"url\", \"type\":\"string\"},\n"
                        + "   {\"name\": \"count\", \"type\":\"integer\"}\n"
                        + " ]\n"
                        + "}";
    GenericSchemaTabular schema = new GenericSchemaTabular(schemaJSON);

    Assert.assertEquals("The schemaType must match.", "twitterTop10", schema.getSchemaType());
    Assert.assertEquals("The schemaVersion must match", "1.0", schema.getSchemaVersion());

    Map<String, Type> expectedFieldToType = Maps.newHashMap();
    expectedFieldToType.put("url", Type.STRING);
    expectedFieldToType.put("count", Type.INTEGER);

    Map<String, Type> fieldToType = schema.getValuesDescriptor().getFieldToType();

    Assert.assertEquals("The field to type must match", expectedFieldToType, fieldToType);
  }
}
