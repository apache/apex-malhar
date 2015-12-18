/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.lib.appdata.dimensions;


import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.lib.appdata.schemas.CustomTimeBucket;
import com.datatorrent.lib.appdata.schemas.TimeBucket;
import com.datatorrent.lib.dimensions.CustomTimeBucketRegistry;

public class CustomTimeBucketRegistryTest
{
  @Test
  public void testBuildingRegistry()
  {
    CustomTimeBucketRegistry timeBucketRegistry = new CustomTimeBucketRegistry();

    CustomTimeBucket c1m = new CustomTimeBucket(TimeBucket.MINUTE);
    CustomTimeBucket c1h = new CustomTimeBucket(TimeBucket.HOUR);
    CustomTimeBucket c1d = new CustomTimeBucket(TimeBucket.DAY);

    timeBucketRegistry.register(c1m, TimeBucket.MINUTE.ordinal());
    timeBucketRegistry.register(c1h, TimeBucket.HOUR.ordinal());
    timeBucketRegistry.register(c1d, TimeBucket.DAY.ordinal());

    CustomTimeBucket customTimeBucket = timeBucketRegistry.getTimeBucket(TimeBucket.MINUTE.ordinal());
    Assert.assertTrue(customTimeBucket.isUnit());
    Assert.assertEquals(TimeBucket.MINUTE, customTimeBucket.getTimeBucket());

    customTimeBucket = timeBucketRegistry.getTimeBucket(TimeBucket.HOUR.ordinal());
    Assert.assertTrue(customTimeBucket.isUnit());
    Assert.assertEquals(TimeBucket.HOUR, customTimeBucket.getTimeBucket());

    customTimeBucket = timeBucketRegistry.getTimeBucket(TimeBucket.DAY.ordinal());
    Assert.assertTrue(customTimeBucket.isUnit());
    Assert.assertEquals(TimeBucket.DAY, customTimeBucket.getTimeBucket());

    Assert.assertEquals(TimeBucket.MINUTE.ordinal(), (int)timeBucketRegistry.getTimeBucketId(c1m));
    Assert.assertEquals(TimeBucket.HOUR.ordinal(), (int)timeBucketRegistry.getTimeBucketId(c1h));
    Assert.assertEquals(TimeBucket.DAY.ordinal(), (int)timeBucketRegistry.getTimeBucketId(c1d));
  }

  @Test
  public void testRegister()
  {
    CustomTimeBucketRegistry timeBucketRegistry = new CustomTimeBucketRegistry();

    CustomTimeBucket c1m = new CustomTimeBucket(TimeBucket.MINUTE);
    CustomTimeBucket c1h = new CustomTimeBucket(TimeBucket.HOUR);
    CustomTimeBucket c1d = new CustomTimeBucket(TimeBucket.DAY);

    timeBucketRegistry.register(c1m, TimeBucket.MINUTE.ordinal());
    timeBucketRegistry.register(c1h, TimeBucket.HOUR.ordinal());
    timeBucketRegistry.register(c1d, TimeBucket.DAY.ordinal());

    int max = Integer.MIN_VALUE;
    max = Math.max(max, TimeBucket.MINUTE.ordinal());
    max = Math.max(max, TimeBucket.HOUR.ordinal());
    max = Math.max(max, TimeBucket.DAY.ordinal());

    CustomTimeBucket c5m = new CustomTimeBucket(TimeBucket.MINUTE, 5L);

    timeBucketRegistry.register(c5m);
    int timeBucketId = timeBucketRegistry.getTimeBucketId(c5m);

    Assert.assertEquals(max + 1, timeBucketId);
  }

}
