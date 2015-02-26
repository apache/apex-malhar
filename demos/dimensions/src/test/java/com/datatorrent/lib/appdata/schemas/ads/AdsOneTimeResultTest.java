/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.ads;

import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.ResultSerializerFactory;
import com.datatorrent.lib.appdata.schemas.ads.AdsOneTimeResult.AdsOneTimeData;
import com.google.common.collect.Lists;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class AdsOneTimeResultTest
{
  private static final Logger logger = LoggerFactory.getLogger(AdsOneTimeResultTest.class);

  public AdsOneTimeResultTest()
  {
  }

  @Test
  public void testSerialization()
  {
    final String id = "js92384142834802";
    final String bucket = "1h";

    final String time1 = "2014-03-01 00:00:00:00";
    final String time2 = "2014-03-01 01:00:00:00";

    final String advertiser = "starbucks";
    final String publisher = "google";

    final long impressions1 = 7882384;
    final long impressions2 = 7232109;

    final long clicks1 = 13942;
    final long clicks2 = 51789;

    final double cost1 = 1.0;
    final double cost2 = 2.0;

    final double revenue1 = 1.0;
    final double revenue2 = 2.0;

    final String testOneTimeData =
              "{"
            + "\"id\":\"" + id + "\","
            + "\"type\":\"oneTimeData\","
            + "\"data\":["
            +   "{"
            +     "\"time\":\"" + time1 + "\","
            +     "\"advertiser\":\"" + advertiser + "\","
            +     "\"publisher\":\"" + publisher + "\","
            +     "\"impressions\":" + impressions1 + ","
            +     "\"clicks\":" + clicks1 + ","
            +     "\"cost\":" + cost1 + ","
            +     "\"revenue\":" + revenue1
            +   "},"
            +   "{"
            +     "\"time\":\"" + time2 + "\","
            +     "\"advertiser\":\"" + advertiser + "\","
            +     "\"publisher\":\"" + publisher + "\","
            +     "\"impressions\":" + impressions2 + ","
            +     "\"clicks\":" + clicks2 + ","
            +     "\"cost\":" + cost2 + ","
            +     "\"revenue\":" + revenue2
            +   "}]"
            + "}";

    Query oneTimeQuery = new Query();
    oneTimeQuery.setId(id);
    oneTimeQuery.setType("oneTimeQuery");

    AdsOneTimeResult aotr = new AdsOneTimeResult(oneTimeQuery);
    List<AdsOneTimeData> data = Lists.newArrayList();
    aotr.setData(data);

    AdsOneTimeData aotd = new AdsOneTimeData();

    aotd.setTime(time1);
    aotd.setAdvertiser(advertiser);
    aotd.setPublisher(publisher);
    aotd.setImpressions(impressions1);
    aotd.setClicks(clicks1);
    aotd.setCost(cost1);
    aotd.setRevenue(revenue1);

    data.add(aotd);

    aotd = new AdsOneTimeData();

    aotd.setTime(time2);
    aotd.setAdvertiser(advertiser);
    aotd.setPublisher(publisher);
    aotd.setImpressions(impressions2);
    aotd.setClicks(clicks2);
    aotd.setCost(cost2);
    aotd.setRevenue(revenue2);

    data.add(aotd);

    ResultSerializerFactory rsf = new ResultSerializerFactory();

    String jsonAOTR = rsf.serialize(aotr);

    logger.debug("Expected: {}", testOneTimeData);
    logger.debug("Actual:   {}", jsonAOTR);

    firstDiff(testOneTimeData, jsonAOTR);

    Assert.assertEquals("Serialized json was not correct", testOneTimeData, jsonAOTR);
  }

  private void firstDiff(String a, String b) {
    int length = a.length();

    if(length < b.length()) {
      length = b.length();
    }

    for(int index = 0;
        index < length;
        index++) {
      if(a.charAt(index) != b.charAt(index)) {
        logger.debug("A string: {}", a.substring(index));
        logger.debug("B string: {}", b.substring(index));

        logger.debug("A string: {}", a.substring(0, index));
        logger.debug("B string: {}", b.substring(0, index));
        return;
      }
    }
  }
}
