/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.schemas;

import com.datatorrent.demos.dimensions.schemas.AdsDataQuery.AdsDataQueryData;
import junit.framework.Assert;
import org.junit.Test;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class AdsDataQueryValidatorTest
{
  @Test
  public void noDataSetTest()
  {
    AdsDataQuery adq = new AdsDataQuery();
    adq.setId("1");
    adq.setType(AdsDataQuery.TYPE);

    AdsDataQueryValidator adqv = new AdsDataQueryValidator();
    Assert.assertFalse("No data set was set.", adqv.validate(adq));
  }

  @Test
  public void noTimeSetTest()
  {
    AdsDataQuery adq = new AdsDataQuery();
    adq.setId("1");
    adq.setType(AdsDataQuery.TYPE);

    AdsDataQueryData adqd = new AdsDataQueryData();
    adq.setData(adqd);
    AdsDataQueryValidator adqv = new AdsDataQueryValidator();
    Assert.assertFalse("No time set.", adqv.validate(adq));
  }

  @Test
  public void invalidTimeTest()
  {
    AdsDataQuery adq = new AdsDataQuery();
    adq.setId("1");
    adq.setType(AdsDataQuery.TYPE);

    AdsDataQueryData adqd = new AdsDataQueryData();
    adq.setData(adqd);

    AdsTimeRangeBucket atrb = new AdsTimeRangeBucket();
    adqd.setTime(atrb);
    atrb.setFrom("a");
    atrb.setTo("b");
    atrb.setBucket(AdsSchemaResult.BUCKETS[0]);

    AdsDataQueryValidator adqv = new AdsDataQueryValidator();
    Assert.assertFalse("No time set.", adqv.validate(adq));
  }

  @Test
  public void invalidTimeTest1()
  {
    AdsDataQuery adq = new AdsDataQuery();
    adq.setId("1");
    adq.setType(AdsDataQuery.TYPE);

    AdsDataQueryData adqd = new AdsDataQueryData();
    adq.setData(adqd);

    AdsTimeRangeBucket atrb = new AdsTimeRangeBucket();
    adqd.setTime(atrb);
    atrb.setFrom("2015-03-05 16:47:00:000");
    atrb.setBucket(AdsSchemaResult.BUCKETS[0]);

    AdsDataQueryValidator adqv = new AdsDataQueryValidator();
    Assert.assertFalse("No time set.", adqv.validate(adq));
  }

  @Test
  public void invalidTimeTest2()
  {
    AdsDataQuery adq = new AdsDataQuery();
    adq.setId("1");
    adq.setType(AdsDataQuery.TYPE);

    AdsDataQueryData adqd = new AdsDataQueryData();
    adq.setData(adqd);

    AdsTimeRangeBucket atrb = new AdsTimeRangeBucket();
    adqd.setTime(atrb);
    atrb.setFrom("2015-03-05 16:47:00:000");
    atrb.setTo("2015-03-05 16:48:00:000");
    atrb.setBucket(AdsSchemaResult.BUCKETS[0]);
    atrb.setLatestNumBuckets(1);

    AdsDataQueryValidator adqv = new AdsDataQueryValidator();
    Assert.assertFalse("Invalid to specify from and to and latest num buckets.", adqv.validate(adq));
  }

  @Test
  public void simpleValidTest()
  {
    AdsDataQuery adq = new AdsDataQuery();
    adq.setId("1");
    adq.setType(AdsDataQuery.TYPE);

    AdsDataQueryData adqd = new AdsDataQueryData();
    adq.setData(adqd);

    AdsTimeRangeBucket atrb = new AdsTimeRangeBucket();
    adqd.setTime(atrb);
    atrb.setFrom("2015-03-05 16:47:00:000");
    atrb.setTo("2015-03-05 16:47:00:000");
    atrb.setBucket(AdsSchemaResult.BUCKETS[0]);

    AdsDataQueryValidator adqv = new AdsDataQueryValidator();
    Assert.assertTrue("This should be valid.", adqv.validate(adq));
  }

  @Test
  public void invalidAdvertiserTest()
  {
    AdsDataQuery adq = new AdsDataQuery();
    adq.setId("1");
    adq.setType(AdsDataQuery.TYPE);

    AdsDataQueryData adqd = new AdsDataQueryData();
    adq.setData(adqd);

    AdsKeys keys = new AdsKeys();
    keys.setAdvertiser("Tim Farkas is the coolest");

    AdsTimeRangeBucket atrb = new AdsTimeRangeBucket();
    adqd.setTime(atrb);
    atrb.setFrom("2015-03-05 16:47:00:000");
    atrb.setTo("2015-03-05 16:47:00:000");

    AdsDataQueryValidator adqv = new AdsDataQueryValidator();
    Assert.assertFalse("Invalid advertiser.", adqv.validate(adq));
  }
}
