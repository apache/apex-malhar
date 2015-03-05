/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.schemas;

import com.datatorrent.lib.appdata.qr.QueryDeserializerFactory;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class AdsDataQueryTest
{
  private static final Logger logger = LoggerFactory.getLogger(AdsDataQueryTest.class);

  @Test
  public void testDeserialization()
  {
    final String id = "1";
    final String type = "dataQuery";

    final String fromTime = "2015-02-20 00:00:00";
    final String toTime = "2015-02-21 00:00:00";
    final String bucket = "1h";

    final String advertiser = "starbucks";
    final String publisher = "google";
    final String location = "SKY";
    final Long countdown = 10L;
    final Boolean incompleteResultOK = false;

    final String json = "{" +
                          "\"id\":\"" + id + "\"," +
                          "\"type\":\"" + type + "\"," +
                          "\"data\":{" +
                          "\"time\":{" +
                          "\"from\":\"" + fromTime + "\"," +
                          "\"to\":\"" + toTime + "\"," +
                          "\"bucket\":\"" + bucket + "\"" +
                          "}," +
                          "\"keys\":{" +
                          "\"advertiser\":\"" + advertiser + "\"," +
                          "\"publisher\":\"" + publisher + "\"," +
                          "\"location\":\"" + location + "\"" +
                          "}" +
                          "}," +
                        "\"countdown\":\"" + countdown + "\"," +
                        "\"incompleteResultOK\":" + incompleteResultOK +
                          "}";

    logger.debug("Query: {}", json);

    @SuppressWarnings("unchecked")
    QueryDeserializerFactory qb = new QueryDeserializerFactory(AdsDataQuery.class);
    AdsDataQuery dq = (AdsDataQuery) qb.deserialize(json);

    Assert.assertEquals("Ids must equal.", id, dq.getId());
    Assert.assertEquals("The types must equal.", type, dq.getType());
    AdsTimeRangeBucket trb = dq.getData().getTime();
    AdsKeys aks = dq.getData().getKeys();
    Assert.assertEquals("The from times must match.", fromTime, trb.getFrom());
    Assert.assertEquals("The to times must match.", toTime, trb.getTo());
    Assert.assertEquals("The bucket must match", bucket, trb.getBucket());
    Assert.assertEquals("The advertiser", advertiser, aks.getAdvertiser());
    Assert.assertEquals("The publisher", publisher, aks.getPublisher());
    Assert.assertEquals("The countdown must equal", countdown, (Long) dq.getCountdown());
    Assert.assertEquals("The countdown must equal", incompleteResultOK, dq.getIncompleteResultOK());
  }
}
