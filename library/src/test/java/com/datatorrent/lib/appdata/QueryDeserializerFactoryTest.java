/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata;

import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.QueryDeserializerFactory;
import com.datatorrent.lib.appdata.schemas.SchemaQuery;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class QueryDeserializerFactoryTest
{
  @Test
  public void testMalformedQuery()
  {
    QueryDeserializerFactory qdf = new QueryDeserializerFactory(SchemaQuery.class);

    String malformed = "{\"}";
    Query query = qdf.deserialize(malformed);

    Assert.assertEquals("Resulting query should be null.", query, null);
  }

  @Test
  public void testUnregisteredQueryType()
  {
    QueryDeserializerFactory qdf = new QueryDeserializerFactory(SchemaQuery.class);

    String unsupportedQuery = "{\"id\":\"1\",\"type\":\"Invalid type\"}";
    Query query = qdf.deserialize(unsupportedQuery);

    Assert.assertEquals("Resulting query should be null.", query, null);
  }
}
