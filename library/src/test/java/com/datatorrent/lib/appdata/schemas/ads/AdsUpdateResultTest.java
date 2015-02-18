/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.ads;

import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.ResultSerializerFactory;
import com.datatorrent.lib.appdata.schemas.ads.AdsUpdateResult.AdsUpdateData;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class AdsUpdateResultTest
{
  private static final Logger logger = LoggerFactory.getLogger(AdsUpdateResultTest.class);

  public AdsUpdateResultTest()
  {
  }

  //@Test
  public void testSerialization()
  {
    final String id = "js34135136425";
    final String type = "updateData";

    final String advertiser = "starbucks";
    final String publisher = "google";

    final String time = "2014-03-01 15:00:00";
    final String bucket = "1h";

    final long impressions = 7882384;
    final long clicks = 13942;

    final String jsonExpected = "{" +
                          "\"id\":\"" + id + "\"," +
                          "\"type\":\"" + type + "\"," +
                          "\"data\":{" +
                            "\"keys\":{" +
                            "\"advertiser\":\"" + advertiser + "\"," +
                            "\"publisher\":\"" + publisher + "\"" +
                          "}," +
                          "\"data\":{" +
                            "\"time\":\"" + time + "\"," +
                            "\"bucket\":\"" + bucket + "\"," +
                            "\"values\":{" +
                              "\"impressions\":" + impressions + "," +
                              "\"clicks\":" + clicks + "" +
                            "}" +
                          "}" +
                        "}}";

    Query query = new Query();
    query.setId(id);
    query.setType(type);

    AdsUpdateResult aurt = new AdsUpdateResult(query);

    AdsUpdateData aud = new AdsUpdateData();

    AdsKeys aks = new AdsKeys();
    aks.setPublisher(publisher);
    aks.setAdvertiser(advertiser);
    aud.setKeys(aks);

    AdsDataData add = new AdsDataData();
    add.setTime(time);
    add.setBucket(bucket);

    AdsDataValues advs = new AdsDataValues();
    advs.setClicks(clicks);
    advs.setImpressions(impressions);
    add.setValues(advs);

    aud.setData(add);

    aurt.setData(aud);

    ResultSerializerFactory rsf = new ResultSerializerFactory();

    String jsonResult = rsf.serialize(aurt);

    logger.debug("Expected: {}", jsonExpected);
    logger.debug("Actual:   {}", jsonResult);

    Assert.assertEquals("The serialized result must match.", jsonExpected, jsonResult);
  }
}
