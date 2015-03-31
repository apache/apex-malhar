/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata;

import com.datatorrent.lib.appdata.qr.Data;
import com.datatorrent.lib.appdata.qr.DataDeserializerFactory;
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
    DataDeserializerFactory qdf = new DataDeserializerFactory(SchemaQuery.class);

    String malformed = "{\"}";
    Data data = qdf.deserialize(malformed);

    Assert.assertEquals("Resulting query should be null.", data, null);
  }

  @Test
  public void testUnregisteredQueryType()
  {
    DataDeserializerFactory qdf = new DataDeserializerFactory(SchemaQuery.class);

    String unsupportedQuery = "{\"id\":\"1\",\"type\":\"Invalid type\"}";
    Data data = qdf.deserialize(unsupportedQuery);

    Assert.assertEquals("Resulting query should be null.", data, null);
  }
}
