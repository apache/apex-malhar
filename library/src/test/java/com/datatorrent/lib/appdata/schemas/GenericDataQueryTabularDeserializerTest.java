/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GenericDataQueryTabularDeserializerTest
{
  @Test
  public void simpleDeserializerTest()
  {
    GenericDataQueryTabularDeserializer deserializer = new GenericDataQueryTabularDeserializer();

    String queryJSON = "{\n"
                       + "   \"id\": \"1\",\n"
                       + "   \"type\": \"dataQuery\",\n"
                       + "   \"data\": {\n"
                       + "      \"fields\": [ \"url\", \"count\" ]\n"
                       + "   }\n"
                       + "}";

    GenericDataQueryTabular gQuery = (GenericDataQueryTabular) deserializer.deserialize(queryJSON, null);

    Assert.assertEquals("The id must equal.", "1", gQuery.getId());
    Assert.assertEquals("The type must equal.", GenericDataQueryTabular.TYPE, gQuery.getType());

    Fields fields = new Fields(Sets.newHashSet("url", "count"));

    Assert.assertEquals("The fields must equal.", fields, gQuery.getFields());
  }
}
