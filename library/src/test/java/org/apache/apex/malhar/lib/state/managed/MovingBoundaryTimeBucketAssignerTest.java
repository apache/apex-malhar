/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.apex.malhar.lib.state.managed;

import java.io.IOException;

import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;

import org.apache.apex.malhar.lib.util.KryoCloneUtils;
import org.apache.commons.lang3.mutable.MutableLong;

public class MovingBoundaryTimeBucketAssignerTest
{

  class TestMeta extends TestWatcher
  {
    MovingBoundaryTimeBucketAssigner timeBucketAssigner;
    MockManagedStateContext mockManagedStateContext;

    @Override
    protected void starting(Description description)
    {
      timeBucketAssigner = new MovingBoundaryTimeBucketAssigner();
      mockManagedStateContext = new MockManagedStateContext(ManagedStateTestUtils.getOperatorContext(9));
    }

    @Override
    protected void finished(Description description)
    {
    }
  }

  @Rule
  public TestMeta testMeta = new TestMeta();

  @Test
  public void testSerde() throws IOException
  {
    MovingBoundaryTimeBucketAssigner deserialized = KryoCloneUtils.cloneObject(testMeta.timeBucketAssigner);
    Assert.assertNotNull("time bucket assigner", deserialized);
  }

  @Test
  public void testNumBuckets()
  {
    testMeta.timeBucketAssigner.setExpireBefore(Duration.standardHours(1));
    testMeta.timeBucketAssigner.setBucketSpan(Duration.standardMinutes(30));

    testMeta.timeBucketAssigner.setup(testMeta.mockManagedStateContext);

    Assert.assertEquals("num buckets", 2, testMeta.timeBucketAssigner.getNumBuckets());
    testMeta.timeBucketAssigner.teardown();
  }

  @Test
  public void testTimeBucketKey()
  {
    testMeta.timeBucketAssigner.setExpireBefore(Duration.standardHours(1));
    testMeta.timeBucketAssigner.setBucketSpan(Duration.standardMinutes(30));

    long referenceTime = testMeta.timeBucketAssigner.getReferenceInstant().getMillis();
    testMeta.timeBucketAssigner.setup(testMeta.mockManagedStateContext);

    long time1 = referenceTime - Duration.standardMinutes(2).getMillis();
    Assert.assertEquals("time bucket", 1, testMeta.timeBucketAssigner.getTimeBucket(time1));

    long time0 = referenceTime - Duration.standardMinutes(40).getMillis();
    Assert.assertEquals("time bucket", 0, testMeta.timeBucketAssigner.getTimeBucket(time0));

    long expiredTime = referenceTime - Duration.standardMinutes(65).getMillis();
    Assert.assertEquals("time bucket", -1, testMeta.timeBucketAssigner.getTimeBucket(expiredTime));
    testMeta.timeBucketAssigner.teardown();
  }

  @Test
  public void testTimeBucketKeyExpiry()
  {
    final MutableLong purgeLessThanEqualTo = new MutableLong(-2);
    testMeta.timeBucketAssigner.setExpireBefore(Duration.standardSeconds(1));
    testMeta.timeBucketAssigner.setBucketSpan(Duration.standardSeconds(1));
    testMeta.timeBucketAssigner.setPurgeListener(new TimeBucketAssigner.PurgeListener()
    {
      @Override
      public void purgeTimeBucketsLessThanEqualTo(long timeBucket)
      {
        purgeLessThanEqualTo.setValue(timeBucket);
      }
    });

    long referenceTime = testMeta.timeBucketAssigner.getReferenceInstant().getMillis();
    testMeta.timeBucketAssigner.setup(testMeta.mockManagedStateContext);
    Assert.assertEquals("purgeLessThanEqualTo", -2L, purgeLessThanEqualTo.longValue());
    long time0 = Duration.standardSeconds(0).getMillis() + referenceTime;
    Assert.assertEquals("time bucket", 1, testMeta.timeBucketAssigner.getTimeBucket(time0) );
    testMeta.timeBucketAssigner.endWindow();
    Assert.assertEquals("purgeLessThanEqualTo", -1, purgeLessThanEqualTo.longValue());

    long time1 = Duration.standardSeconds(9).getMillis() + referenceTime;
    Assert.assertEquals("time bucket", 10, testMeta.timeBucketAssigner.getTimeBucket(time1) );
    testMeta.timeBucketAssigner.endWindow();
    Assert.assertEquals("purgeLessThanEqualTo", 8, purgeLessThanEqualTo.longValue());

    long time2 = Duration.standardSeconds(10).getMillis()  + referenceTime;
    Assert.assertEquals("time bucket", 11, testMeta.timeBucketAssigner.getTimeBucket(time2) );
    testMeta.timeBucketAssigner.endWindow();
    Assert.assertEquals("purgeLessThanEqualTo", 9, purgeLessThanEqualTo.longValue());

    //Check for expiry of time1 now
    Assert.assertEquals("time bucket", -1, testMeta.timeBucketAssigner.getTimeBucket(time1) );
    testMeta.timeBucketAssigner.endWindow();
    Assert.assertEquals("purgeLessThanEqualTo", 9, purgeLessThanEqualTo.longValue());

    testMeta.timeBucketAssigner.teardown();
  }
}
