/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata;

import com.datatorrent.lib.appdata.schemas.SchemaQuery;
import org.junit.Assert;
import org.junit.Test;

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
                                      "\"type\":\"" + SchemaQuery.SCHEMA_QUERY_TYPE + "\"" +
                                    "}";

    @SuppressWarnings("unchecked")
    QueryDeserializerFactory qb = new QueryDeserializerFactory(SchemaQuery.class);

    SchemaQuery schemaQuery = (SchemaQuery) qb.deserialize(schemaQueryJSON);

    Assert.assertEquals("Id's must match", id, schemaQuery.getId());
    Assert.assertEquals("Types must match", SchemaQuery.SCHEMA_QUERY_TYPE, schemaQuery.getType());
  }
}
