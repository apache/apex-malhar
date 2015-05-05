/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.datatorrent.lib.appdata.qr.DataDeserializerFactory;
import com.datatorrent.lib.appdata.schemas.SchemaQuery;
import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class SchemaQueryTest
{
  @Test
  public void jsonToSchemaQueryTest()
  {
    final String id = "12345";
    final String schemaQueryJSON = "{" +
                                      "\"id\":\"" + id + "\"," +
                                      "\"type\":\"" + SchemaQuery.TYPE + "\"" +
                                    "}";



    @SuppressWarnings("unchecked")
    DataDeserializerFactory qb = new DataDeserializerFactory(SchemaQuery.class);

    SchemaQuery schemaQuery = (SchemaQuery) qb.deserialize(schemaQueryJSON);

    Assert.assertEquals("Id's must match", id, schemaQuery.getId());
    Assert.assertEquals("Types must match", SchemaQuery.TYPE, schemaQuery.getType());
  }

  @Test
  public void jsonToSchemaQueryWithSchemaKeysTest()
  {
    final Map<String, String> expectedSchemaKeys = Maps.newHashMap();
    expectedSchemaKeys.put("publisher", "google");
    expectedSchemaKeys.put("advertiser", "microsoft");
    expectedSchemaKeys.put("location", "CA");

    final String id = "12345";
    final String schemaQueryJSON = "{" +
                                      "\"id\":\"" + id + "\"," +
                                      "\"type\":\"" + SchemaQuery.TYPE + "\"," +
                                      "\"schemaKeys\":" +
                                      "{\"publisher\":\"google\",\"advertiser\":\"microsoft\",\"location\":\"CA\"}" +
                                   "}";

    @SuppressWarnings("unchecked")
    DataDeserializerFactory qb = new DataDeserializerFactory(SchemaQuery.class);

    SchemaQuery schemaQuery = (SchemaQuery) qb.deserialize(schemaQueryJSON);

    Assert.assertEquals("Id's must match", id, schemaQuery.getId());
    Assert.assertEquals("Types must match", SchemaQuery.TYPE, schemaQuery.getType());
    Assert.assertEquals("Schema keys must match", expectedSchemaKeys, schemaQuery.getSchemaKeys());
  }
}
