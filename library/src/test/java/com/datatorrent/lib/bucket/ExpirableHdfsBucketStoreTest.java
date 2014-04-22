/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.bucket;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExpirableHdfsBucketStoreTest
{
  static BucketStoreTests tests;
  static ExpirableHdfsBucketStore<DummyEvent> expirableStore;

  @BeforeClass
  public static void setUp()
  {
    tests = new BucketStoreTests();
    expirableStore = new ExpirableHdfsBucketStore<DummyEvent>();
    tests.setup(expirableStore);
  }

  @AfterClass
  public static void tearDown() throws Exception
  {
    tests.tearDown();
  }

  @Test
  public void test1() throws Exception
  {
    tests.testStoreBucketData();
    expirableStore.deleteExpiredBuckets(1);
    Assert.assertTrue(!tests.bucketExists(0));
  }
}
