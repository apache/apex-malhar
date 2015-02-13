/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.ads;

import com.datatorrent.lib.appdata.qr.QueryDeserializerFactory;
import com.datatorrent.lib.appdata.schemas.TimeRangeBucket;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class AdsOneTimeQueryTest
{
  @Test
  public void testDeserialization()
  {
    final String id = "js92384142834802";
    final String type = "oneTimeQuery";

    final String fromTime = "2014-03-01 00:00:00";
    final String toTime = "2014-03-01 12:00:00";
    final String bucket = "1h";

    final String advertiser = "starbucks";
    final String publisher = "google";

    final String json = "{\n" +
                        "   \"id\": \"" + id + "\",\n" +
                        "   \"type\": \"" + type + "\",\n" +
                        "   \"data\": { \n" +
                        "      \"time\": {\n" +
                        "	   \"from\": \"" + fromTime + "\", \n" +
                        "	   \"to\": \"" + toTime + "\", \n" +
                        "	   \"bucket\": \"" + bucket + "\"\n" +
                        "      },\n" +
                        "      \"keys\": {\n" +
                        "   \"advertiser\": \"" + advertiser + "\",\n" +
                        "   \"publisher\": \"" + publisher + "\" \n" +
                        "      }\n" +
                        "   } \n" +
                        "}";


    @SuppressWarnings("unchecked")
    QueryDeserializerFactory qb = new QueryDeserializerFactory(AdsOneTimeQuery.class);

    AdsOneTimeQuery aotq = (AdsOneTimeQuery) qb.deserialize(json);

    Assert.assertEquals("Ids must equal.", id, aotq.getId());
    Assert.assertEquals("The types must equal.", type, aotq.getType());
    TimeRangeBucket trb = aotq.getData().getTime();
    AdsKeys aks = aotq.getData().getKeys();
    Assert.assertEquals("The from times must match.", fromTime, trb.getFrom());
    Assert.assertEquals("The to times must match.", toTime, trb.getTo());
    Assert.assertEquals("The bucket must match", bucket, trb.getBucket());
    Assert.assertEquals("The advertiser", advertiser, aks.getAdvertiser());
    Assert.assertEquals("The publisher", publisher, aks.getPublisher());
  }
}
