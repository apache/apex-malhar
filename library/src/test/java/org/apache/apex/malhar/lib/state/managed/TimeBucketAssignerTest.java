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

import com.datatorrent.lib.util.KryoCloneUtils;

public class TimeBucketAssignerTest
{

  class TestMeta extends TestWatcher
  {
    TimeBucketAssigner timeBucketAssigner;
    MockManagedStateContext mockManagedStateContext;

    @Override
    protected void starting(Description description)
    {
      timeBucketAssigner = new TimeBucketAssigner();
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
    TimeBucketAssigner deserialized = KryoCloneUtils.cloneObject(testMeta.timeBucketAssigner);
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
    Assert.assertEquals("time bucket", 1, testMeta.timeBucketAssigner.getTimeBucketAndAdjustBoundaries(time1));

    long time0 = referenceTime - Duration.standardMinutes(40).getMillis();
    Assert.assertEquals("time bucket", 0, testMeta.timeBucketAssigner.getTimeBucketAndAdjustBoundaries(time0));

    long expiredTime = referenceTime - Duration.standardMinutes(65).getMillis();
    Assert.assertEquals("time bucket", -1, testMeta.timeBucketAssigner.getTimeBucketAndAdjustBoundaries(expiredTime));
    testMeta.timeBucketAssigner.teardown();
  }

  @Test
  public void testTimeBucketKeyExpiry()
  {
    testMeta.timeBucketAssigner.setExpireBefore(Duration.standardSeconds(1));
    testMeta.timeBucketAssigner.setBucketSpan(Duration.standardSeconds(1));

    long referenceTime = testMeta.timeBucketAssigner.getReferenceInstant().getMillis();
    testMeta.timeBucketAssigner.setup(testMeta.mockManagedStateContext);

    long time1 = Duration.standardSeconds(9).getMillis() + referenceTime;
    Assert.assertEquals("time bucket", 10, testMeta.timeBucketAssigner.getTimeBucketAndAdjustBoundaries(time1) );

    long time2 = Duration.standardSeconds(10).getMillis()  + referenceTime;
    Assert.assertEquals("time bucket", 11, testMeta.timeBucketAssigner.getTimeBucketAndAdjustBoundaries(time2) );

    //Check for expiry of time1 now
    Assert.assertEquals("time bucket", -1, testMeta.timeBucketAssigner.getTimeBucketAndAdjustBoundaries(time1) );

    testMeta.timeBucketAssigner.teardown();
  }
}
