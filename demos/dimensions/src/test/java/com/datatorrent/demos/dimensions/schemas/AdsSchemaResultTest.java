/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.schemas;

import com.datatorrent.demos.dimensions.ads.schemas.AdsSchemaResult;
import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.DataSerializerFactory;
import com.datatorrent.lib.appdata.schemas.SchemaQuery;
import com.datatorrent.lib.appdata.schemas.SchemaTestUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class AdsSchemaResultTest
{
  private static final Logger logger = LoggerFactory.getLogger(AdsSchemaResultTest.class);

  public AdsSchemaResultTest()
  {
  }

  @Test
  public void testSerialization()
  {
    final String id = "js134232342134";
    final String type = "schemaData";
    final String schemaType = "dimensions";
    final String schemaVersion = "1.0";

    Query oneTimeQuery = new Query();
    oneTimeQuery.setId(id);
    oneTimeQuery.setType("oneTimeQuery");

    final String jsonExpected = "{" +
    "\"id\":\"" + id + "\"," +
    "\"type\":\"" + type + "\"," +
    "\"data\":{" +
      "\"schemaType\":\"" + schemaType + "\"," +
      "\"schemaVersion\":\"" + schemaVersion + "\"," +
      "\"timeBuckets\":{" +
      "\"from\":\"" + AdsSchemaResult.FROM + "\"," +
      "\"to\":\"" + AdsSchemaResult.TO + "\"," +
      "\"buckets\":[" + StringUtils.join(SchemaTestUtils.wrap(AdsSchemaResult.BUCKETS, "\""), ",") +"]" +
      "}," +
    "\"keys\":[" + "{" +
      "\"name\":\"publisher\"," +
      "\"keyValues\":[" + StringUtils.join(SchemaTestUtils.wrap(AdsSchemaResult.PUBLISHERS, "\""), ",") + "]" +
    "}," +
    "{" +
      "\"name\":\"advertiser\"," +
      "\"keyValues\":[" + StringUtils.join(SchemaTestUtils.wrap(AdsSchemaResult.ADVERTISERS, "\""), ",") + "]" +
    "}," +
    "{" +
      "\"name\":\"location\"," +
      "\"keyValues\":[" + StringUtils.join(SchemaTestUtils.wrap(AdsSchemaResult.LOCATIONS, "\""), ",") + "]" +
    "}" +
    "]," +
    "\"values\":[" +
      "{\"name\":\"impressions\",\"type\":\"integer\"}," +
      "{\"name\":\"clicks\",\"type\":\"integer\"}," +
      "{\"name\":\"cost\",\"type\":\"float\"}," +
      "{\"name\":\"revenue\",\"type\":\"float\"}" + "]" +
    "}" +
    "}";

    SchemaQuery sq = new SchemaQuery();

    sq.setId(id);
    sq.setType(SchemaQuery.TYPE);

    AdsSchemaResult asr = new AdsSchemaResult(sq);

    DataSerializerFactory rsf = new DataSerializerFactory();

    String jsonResult = rsf.serialize(asr);

    logger.debug("Expected: {}", jsonExpected);
    logger.debug("Actual:   {}", jsonResult);

    Assert.assertEquals("Serialized json was not correct", jsonExpected, jsonResult);
  }
}
