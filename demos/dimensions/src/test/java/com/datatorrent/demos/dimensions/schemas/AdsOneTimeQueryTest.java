/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.schemas;

import com.datatorrent.demos.dimensions.schemas.AdsOneTimeQuery;
import com.datatorrent.lib.appdata.qr.QueryDeserializerFactory;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class AdsOneTimeQueryTest
{
  private static final Logger logger = LoggerFactory.getLogger(AdsOneTimeQueryTest.class);

  @Test
  public void testDeserialization()
  {
    final String id = "1";
    final String type = "oneTimeQuery";

    final String fromTime = "2015-02-20 00:00:00";
    final String toTime = "2015-02-21 00:00:00";
    final String bucket = "1h";

    final String advertiser = "starbucks";
    final String publisher = "google";
    final String location = "SKY";

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
                          "}" +
                          "}";

    logger.debug("Query: {}", json);

    @SuppressWarnings("unchecked")
    QueryDeserializerFactory qb = new QueryDeserializerFactory(AdsOneTimeQuery.class);

    AdsOneTimeQuery aotq = (AdsOneTimeQuery) qb.deserialize(json);

    Assert.assertEquals("Ids must equal.", id, aotq.getId());
    Assert.assertEquals("The types must equal.", type, aotq.getType());
    /*TimeRangeBucket trb = aotq.getData().getTime();
    AdsKeys aks = aotq.getData().getKeys();
    Assert.assertEquals("The from times must match.", fromTime, trb.getFrom());
    Assert.assertEquals("The to times must match.", toTime, trb.getTo());
    Assert.assertEquals("The bucket must match", bucket, trb.getBucket());
    Assert.assertEquals("The advertiser", advertiser, aks.getAdvertiser());
    Assert.assertEquals("The publisher", publisher, aks.getPublisher());*/
  }
}
