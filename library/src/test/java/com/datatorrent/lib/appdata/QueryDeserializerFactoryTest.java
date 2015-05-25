/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata;

import com.datatorrent.lib.appdata.query.serde.Message;
import com.datatorrent.lib.appdata.query.serde.MessageDeserializerFactory;
import com.datatorrent.lib.appdata.schemas.SchemaQuery;
import java.io.IOException;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class QueryDeserializerFactoryTest
{
  @Test
  public void testMalformedQuery()
  {
    @SuppressWarnings("unchecked")
    MessageDeserializerFactory qdf = new MessageDeserializerFactory(SchemaQuery.class);

    String malformed = "{\"}";
    boolean exception = false;

    try {
      qdf.deserialize(malformed);
    }
    catch(IOException e) {
      exception = true;
    }

    Assert.assertTrue("Resulting query should throw IOException.", exception);
  }

  @Test
  public void testUnregisteredQueryType()
  {
    @SuppressWarnings("unchecked")
    MessageDeserializerFactory qdf = new MessageDeserializerFactory(SchemaQuery.class);

    String unsupportedQuery = "{\"id\":\"1\",\"type\":\"Invalid type\"}";
    boolean exception = false;

    Message data = null;

    try {
      data = qdf.deserialize(unsupportedQuery);
    }
    catch(IOException e) {
      exception = true;
    }

    Assert.assertTrue("Resulting query should be null.", exception);
  }

  private static final Logger LOG = LoggerFactory.getLogger(QueryDeserializerFactoryTest.class);
}
