/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.twitter;

import com.datatorrent.lib.appdata.qr.ResultSerializerFactory;
import com.datatorrent.lib.appdata.schemas.SchemaQuery;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class TwitterSchemaResultTest
{
  private static final Logger logger = LoggerFactory.getLogger(TwitterSchemaResultTest.class);

  @Test
  public void serializeTest()
  {
    final String id = "js134232342134";
    final String type = "schemaData";

    SchemaQuery sq = new SchemaQuery();
    sq.setId(id);
    sq.setType(SchemaQuery.TYPE);

    final String jsonExpected = "{" +
    "\"id\":\"" + id + "\"," +
    "\"type\":\"" + type + "\"," +
    "\"data\":{" +
      "\"schemaType\":\"" + TwitterSchemaResult.SCHEMA_TYPE + "\"," +
      "\"schemaVersion\":\"" + TwitterSchemaResult.SCHEMA_VERSION + "\"," +
    "\"values\":[" +
      "{\"name\":\"" + TwitterSchemaResult.URL + "\",\"type\":\"" + TwitterSchemaResult.URL_TYPE + "\"}," +
      "{\"name\":\"" + TwitterSchemaResult.COUNT + "\",\"type\":\"" + TwitterSchemaResult.COUNT_TYPE + "\"}" +"]" +
    "}" +
    "}";

    ResultSerializerFactory rsf = new ResultSerializerFactory();

    String jsonResult = rsf.serialize(new TwitterSchemaResult(sq));

    logger.debug("Expected: {}", jsonExpected);
    logger.debug("Actual  : {}", jsonResult);

    Assert.assertEquals("Serialized schema should match.", jsonExpected, jsonResult);
  }
}
