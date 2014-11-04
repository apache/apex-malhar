/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.hds;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hds.HDSReader.HDSQuery;
import com.datatorrent.lib.util.TestUtils;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;

public class HDSReaderTest
{
  @Rule
  public final TestUtils.TestInfo testInfo = new TestUtils.TestInfo();

  private void writeKey(HDSFileAccess fa, Slice key, String data) throws Exception
  {
    HDSWriter hds = new HDSWriter();
    hds.setFileStore(fa);
    hds.setFlushSize(0); // flush after every key

    hds.setup(null);
    hds.writeExecutor = MoreExecutors.sameThreadExecutor(); // synchronous flush on endWindow
    hds.beginWindow(1);
    hds.put(HDSTest.getBucketKey(key), key, data.getBytes());
    hds.endWindow();
    hds.teardown();
  }

  @Test
  public void testReader() throws Exception
  {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    HDSFileAccessFSImpl fa = new MockFileAccess();
    fa.setBasePath(file.getAbsolutePath());

    Slice key0 = HDSTest.newKey(1, 0);
    String data0 = "data0";

    Slice key1 = HDSTest.newKey(1, 1);
    String data1 = "data1";

    writeKey(fa, key0, data0);
    writeKey(fa, key1, data1);

    // setup the reader instance
    final List<HDSQuery> results = Lists.newArrayList();
    HDSReader reader = new HDSReader() {
      @Override
      protected void emitQueryResult(HDSQuery query)
      {
        results.add(query);
      }
    };
    reader.queryExecutor = MoreExecutors.sameThreadExecutor(); // synchronous endWindow processing
    reader.setFileStore(fa);

    reader.setup(null);
    reader.beginWindow(1);

    HDSQuery q = new HDSQuery();
    q.bucketKey = HDSTest.getBucketKey(key1);
    q.keepAliveCount = 1;
    q.key = key1;

    reader.addQuery(q);
    Assert.assertNull("query result before endWindow", q.result);
    Assert.assertEquals("query results", 0, results.size());

    reader.endWindow(); // process query

    Assert.assertArrayEquals("query result after endWindow", data1.getBytes(), q.result);
    Assert.assertEquals("query results", 1, results.size());

    reader.beginWindow(2);
    reader.endWindow(); // emit result again

    Assert.assertEquals("query results", 2, results.size());

    reader.beginWindow(3);
    reader.endWindow(); // emit nothing - query expired

    Assert.assertEquals("query expired", 2, results.size());

    // unknown key
    results.clear();

    Slice key2 = HDSTest.newKey(1, 2);
    HDSQuery q2 = new HDSQuery();
    q2.bucketKey = HDSTest.getBucketKey(key2);
    q2.keepAliveCount = 1;
    q2.key = key2;

    reader.beginWindow(4);
    reader.addQuery(q2);
    reader.endWindow(); // emit nothing - unknown key
    Assert.assertEquals("query results " + results, 1, results.size());
    Assert.assertEquals("query result " + results.get(0), key2, results.get(0).key);
    Assert.assertEquals("query result " + results.get(0), null, results.get(0).result);

    reader.teardown();
  }

  @Test
  public void testReaderRetry() throws Exception
  {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    HDSFileAccessFSImpl fa = new MockFileAccess();
    fa.setBasePath(file.getAbsolutePath());

    Slice key = HDSTest.newKey(1, 1);
    String data = "data1";

    writeKey(fa, key, data);

    // setup the reader instance
    final List<HDSQuery> results = Lists.newArrayList();
    HDSReader reader = new HDSReader() {
      @Override
      protected void emitQueryResult(HDSQuery query)
      {
        results.add(query);
      }
    };
    reader.queryExecutor = MoreExecutors.sameThreadExecutor(); // synchronous endWindow processing
    reader.setFileStore(fa);

    reader.setup(null);
    reader.beginWindow(1);

    HDSQuery q = new HDSQuery();
    q.bucketKey = HDSTest.getBucketKey(key);
    q.keepAliveCount = 2;
    q.key = key;

    reader.addQuery(q);
    Assert.assertNull("query result before endWindow", q.result);
    Assert.assertEquals("query results", 0, results.size());

    reader.endWindow(); // process query

    Assert.assertArrayEquals("query result after endWindow", data.getBytes(), q.result);
    Assert.assertEquals("query results", 1, results.size());
    results.clear();
    q.processed = false;

    String data2 = "data2";
    writeKey(fa, key, data2);

    reader.beginWindow(2);
    reader.endWindow(); // process query

    Assert.assertEquals("query results", 1, results.size());
    Assert.assertArrayEquals("query result after endWindow", data2.getBytes(), q.result);

    reader.teardown();

  }
}
