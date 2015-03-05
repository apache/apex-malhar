/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.schemas;

import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.ResultSerializerFactory;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class AdsDataResultTest
{
  private static final Logger logger = LoggerFactory.getLogger(AdsDataResultTest.class);

  @Test
  public void testSerialization()
  {
    final String id = "js34135136425";
    final String type = "dataResult";

    final String advertiser = "starbucks";
    final String publisher = "google";
    final String location = "SKY";

    final String time = "2014-03-01 15:00:00";
    final String bucket = "1h";

    final long countdown = 1;
    final long impressions = 7882384;
    final long clicks = 13942;
    final double cost = 0.0;
    final double revenue = 0.0;

    final String jsonExpected = "{" +
                          "\"id\":\"" + id + "\"," +
                          "\"type\":\"" + type + "\"," +
                          "\"data\":[{" +
                            "\"time\":\"" + time + "\"," +
                            "\"advertiser\":\"" + advertiser + "\"," +
                            "\"publisher\":\"" + publisher + "\"," +
                            "\"location\":\"" + location + "\"," +
                            "\"impressions\":" + impressions + "," +
                            "\"clicks\":" + clicks + "," +
                            "\"cost\":" + cost + "," +
                            "\"revenue\":" + revenue +
                            "}]," +
                          "\"countdown\":" + countdown +
                          "}";

    Query query = new Query();
    query.setId(id);
    query.setType(type);

    AdsDataResult adr = new AdsDataResult(query);

    ArrayList<AdsDataResult.AdsOneTimeData> auds = new ArrayList<AdsDataResult.AdsOneTimeData>();
    AdsDataResult.AdsOneTimeData aud = new AdsDataResult.AdsOneTimeData();

    aud.setTime(time);
    aud.setPublisher(publisher);
    aud.setAdvertiser(advertiser);
    aud.setLocation(location);
    aud.setImpressions(impressions);
    aud.setClicks(clicks);
    aud.setCost(cost);
    aud.setRevenue(revenue);
    auds.add(aud);

    adr.setData(auds);
    adr.setCountdown(countdown);

    ResultSerializerFactory rsf = new ResultSerializerFactory();

    String jsonResult = rsf.serialize(adr);

    logger.debug("Expected: {}", jsonExpected);
    logger.debug("Actual:   {}", jsonResult);

    Assert.assertEquals("The serialized result must match.", jsonExpected, jsonResult);
  }
}
