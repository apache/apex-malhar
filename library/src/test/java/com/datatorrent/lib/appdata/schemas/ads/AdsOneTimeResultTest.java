/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.ads;

import com.datatorrent.lib.appdata.qr.ResultSerializerFactory;
import com.datatorrent.lib.appdata.schemas.OneTimeQuery;
import com.datatorrent.lib.appdata.schemas.TimeRangeBucket;
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
    final String fromTime = "2014-03-01 00:00:00";
    final String toTime = "2014-03-01 12:00:00";
    final String bucket = "1h";

    final String advertiser = "starbucks";
    final String publisher = "google";

    final String time1 = fromTime;
    final String time2 = "2014-03-01 01:00:00";

    final long impressions1 = 7882384;
    final long impressions2 = 7232109;

    final long clicks1 = 13942;
    final long clicks2 = 51789;

    OneTimeQuery oneTimeQuery = new OneTimeQuery();
    oneTimeQuery.setId(id);
    oneTimeQuery.setType("oneTimeQuery");

    final String testOneTimeData =
              "{"
            + "\"id\":\"" + id + "\","
            + "\"type\":\"oneTimeData\","
            + "\"data\":{"
            +   "\"time\":{"
            +     "\"from\":\"" + fromTime + "\","
            +     "\"to\":\"" + toTime + "\","
            +     "\"bucket\":\"" + bucket + "\""
            +   "},"
            +   "\"keys\":{"
            +     "\"advertiser\":\"" + advertiser + "\","
            +     "\"publisher\":\"" + publisher + "\""
            +   "},"
            +   "\"data\":["
            +   "{"
            +     "\"time\":\"" + time1 + "\","
            +     "\"bucket\":\"" + bucket + "\","
            +     "\"values\":{"
            +       "\"impressions\":" + impressions1 + ","
            +       "\"clicks\":" + clicks1
            +     "}"
            +   "},"
            +   "{"
            +     "\"time\":\"" + time2 + "\","
            +     "\"bucket\":\"" + bucket + "\","
            +     "\"values\":{"
            +       "\"impressions\":" + impressions2 + ","
            +       "\"clicks\":" + clicks2
            +     "}"
            +   "}]"
            + "}"
            + "}";

    AdsOneTimeResult aotr = new AdsOneTimeResult(oneTimeQuery);
    aotr.setType("oneTimeData");

    AdsOneTimeData aotd = new AdsOneTimeData();

      AdsKeys ak = new AdsKeys();
      ak.setAdvertiser(advertiser);
      ak.setPublisher(publisher);

    aotd.setKeys(ak);

      TimeRangeBucket trb = new TimeRangeBucket();
      trb.setFrom(fromTime);
      trb.setTo(toTime);
      trb.setBucket(bucket);

    aotd.setTime(trb);

      List<AdsDataData> adsDataDatas = Lists.newArrayList();

      AdsDataData add = new AdsDataData();
      add.setTime(time1);
      add.setBucket(bucket);
        AdsDataValues advs = new AdsDataValues();
        advs.setClicks(clicks1);
        advs.setImpressions(impressions1);
      add.setValues(advs);

      adsDataDatas.add(add);

      add = new AdsDataData();
      add.setTime(time2);
      add.setBucket(bucket);
        advs = new AdsDataValues();
        advs.setClicks(clicks2);
        advs.setImpressions(impressions2);
      add.setValues(advs);

      adsDataDatas.add(add);

    aotd.setData(adsDataDatas);
    aotr.setData(aotd);

    ResultSerializerFactory rsf = new ResultSerializerFactory();

    String jsonAOTR = rsf.serialize(aotr);

    logger.debug("Expected: {}", testOneTimeData);
    logger.debug("Actual:   {}", jsonAOTR);

    Assert.assertEquals("Serialized json was not correct", testOneTimeData, jsonAOTR);
  }
}
