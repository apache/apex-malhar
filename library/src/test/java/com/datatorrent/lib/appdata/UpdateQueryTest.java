/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata;

import com.datatorrent.lib.appdata.schemas.UpdateQuery;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class UpdateQueryTest
{
  private static final Logger logger = LoggerFactory.getLogger(UpdateQueryTest.class);

  @Test
  public void withQueryTest()
  {
    final String id = "12345";
    final String queryJSON = "{" +
                                      "\"id\":\"" + id + "\"," +
                                      "\"type\":\"" + UpdateQuery.UPDATE_QUERY_TYPE + "\"," +
                                      "\"data\":{\"time\":{\"bucket\":\"1h\"}}" +
                                   "}";

    QueryDeserializerFactory qb = new QueryDeserializerFactory(UpdateQuery.class);
    UpdateQuery uq = (UpdateQuery) qb.deserialize(queryJSON);

    Assert.assertEquals("Id's must match", id, uq.getId());
    Assert.assertEquals("Types must match", UpdateQuery.UPDATE_QUERY_TYPE, uq.getType());
    Assert.assertEquals("Bucket must match", "1h", uq.getData().getTime().getBucket());
  }

  @Test
  public void withoutQueryTest()
  {
    final String id = "12345";
    final String queryJSON = "{" +
                                      "\"id\":\"" + id + "\"," +
                                      "\"type\":\"" + UpdateQuery.UPDATE_QUERY_TYPE + "\"" +
                                   "}";

    QueryDeserializerFactory qb = new QueryDeserializerFactory(UpdateQuery.class);
    UpdateQuery uq = (UpdateQuery) qb.deserialize(queryJSON);

    Assert.assertEquals("Id's must match", id, uq.getId());
    Assert.assertEquals("Types must match", UpdateQuery.UPDATE_QUERY_TYPE, uq.getType());
    Assert.assertTrue("Maps must both be null", null == uq.getData());
  }
}
