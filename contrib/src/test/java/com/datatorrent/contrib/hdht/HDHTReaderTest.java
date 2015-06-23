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
package com.datatorrent.contrib.hdht;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import com.datatorrent.netlet.util.Slice;
import com.datatorrent.contrib.hdht.HDHTFileAccess;
import com.datatorrent.contrib.hdht.HDHTFileAccessFSImpl;
import com.datatorrent.contrib.hdht.HDHTReader;
import com.datatorrent.contrib.hdht.HDHTWriter;
import com.datatorrent.contrib.hdht.HDHTReader.HDSQuery;
import com.datatorrent.lib.util.TestUtils;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;

public class HDHTReaderTest
{
  @Rule
  public final TestUtils.TestInfo testInfo = new TestUtils.TestInfo();

  private void writeKey(HDHTFileAccess fa, Slice key, String data) throws Exception
  {
    HDHTWriter hds = new HDHTWriter();
    hds.setFileStore(fa);
    hds.setFlushSize(0); // flush after every key

    hds.setup(null);
    hds.writeExecutor = MoreExecutors.sameThreadExecutor(); // synchronous flush on endWindow
    hds.beginWindow(1);
    hds.put(HDHTWriterTest.getBucketKey(key), key, data.getBytes());
    hds.endWindow();
    hds.checkpointed(1);
    hds.committed(1);
    hds.teardown();
  }

  @Test
  public void testReader() throws Exception
  {
    File file = new File(testInfo.getDir());
    FileUtils.deleteDirectory(file);

    HDHTFileAccessFSImpl fa = new MockFileAccess();
    fa.setBasePath(file.getAbsolutePath());

    Slice key0 = HDHTWriterTest.newKey(1, 0);
    String data0 = "data0";

    Slice key1 = HDHTWriterTest.newKey(1, 1);
    String data1 = "data1";

    writeKey(fa, key0, data0);
    writeKey(fa, key1, data1);

    // setup the reader instance
    final List<HDSQuery> results = Lists.newArrayList();
    HDHTReader reader = new HDHTReader() {
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
    q.bucketKey = HDHTWriterTest.getBucketKey(key1);
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

    Slice key2 = HDHTWriterTest.newKey(1, 2);
    HDSQuery q2 = new HDSQuery();
    q2.bucketKey = HDHTWriterTest.getBucketKey(key2);
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

    HDHTFileAccessFSImpl fa = new MockFileAccess();
    fa.setBasePath(file.getAbsolutePath());

    Slice key = HDHTWriterTest.newKey(1, 1);
    String data = "data1";

    writeKey(fa, key, data);

    // setup the reader instance
    final List<HDSQuery> results = Lists.newArrayList();
    HDHTReader reader = new HDHTReader() {
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
    q.bucketKey = HDHTWriterTest.getBucketKey(key);
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
