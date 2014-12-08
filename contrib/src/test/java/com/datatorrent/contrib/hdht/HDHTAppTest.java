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
import java.nio.ByteBuffer;
import java.util.TreeMap;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.util.Slice;
import com.datatorrent.contrib.hdht.HDHTFileAccessFSImpl;
import com.datatorrent.lib.util.KeyValPair;

@ApplicationAnnotation(name="HDHTAppTest")
public class HDHTAppTest implements StreamingApplication
{
  private static final String DATA0 = "data0";
  private static final String DATA1 = "data1";
  private static byte[] KEY0 = ByteBuffer.allocate(16).putLong(0).putLong(0x00000000).array();
  private static byte[] KEY1 = ByteBuffer.allocate(16).putLong(0).putLong(0xffffffff).array();

  public static class Generator extends BaseOperator implements InputOperator
  {
    public transient DefaultOutputPort<KeyValPair<byte[], byte[]>> output = new DefaultOutputPort<KeyValPair<byte[], byte[]>>();

    @Override
    public void emitTuples()
    {
      output.emit(new KeyValPair<byte[], byte[]>(KEY0, DATA0.getBytes()));
      output.emit(new KeyValPair<byte[], byte[]>(KEY1, DATA1.getBytes()));
    }
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    Generator generator = dag.addOperator("Generator", new Generator());
    HDHTTestOperator store = dag.addOperator("Store", new HDHTTestOperator());
    store.setFileStore(new MockFileAccess());
    dag.addStream("Generator2Store", generator.output, store.input);
  }

  @Test
  public void test() throws Exception
  {
    File file = new File("target/hds2");
    FileUtils.deleteDirectory(file);

    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.set("dt.operator.Store.fileStore.basePath", file.toURI().toString());
    //conf.set("dt.operator.Store.flushSize", "0");
    conf.set("dt.operator.Store.flushIntervalCount", "1");
    conf.set("dt.operator.Store.partitionCount", "2");

    lma.prepareDAG(new HDHTAppTest(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);
    lc.runAsync();

    long tms = System.currentTimeMillis();
    File f0 = new File(file, "0/0-0");
    File f1 = new File(file, "1/1-0");
    File wal0 = new File(file, "0/_WAL-0");
    File wal1 = new File(file, "1/_WAL-0");

    while (System.currentTimeMillis() - tms < 30000) {
      if (f0.exists() && f1.exists()) break;
      Thread.sleep(100);
    }
    lc.shutdown();

    Assert.assertTrue("exists " + f0, f0.exists() && f0.isFile());
    Assert.assertTrue("exists " + f1, f1.exists() && f1.isFile());
    Assert.assertTrue("exists " + wal0, wal0.exists() && wal0.exists());
    Assert.assertTrue("exists " + wal1, wal1.exists() && wal1.exists());

    HDHTFileAccessFSImpl fs = new MockFileAccess();
    fs.setBasePath(file.toURI().toString());
    fs.init();

    TreeMap<Slice, byte[]> data = new TreeMap<Slice, byte[]>(new HDHTWriterTest.SequenceComparator());
    fs.getReader(0, "0-0").readFully(data);
    Assert.assertArrayEquals("read key=" + new String(KEY0), DATA0.getBytes(), data.get(new Slice(KEY0)));

    data.clear();
    fs.getReader(1, "1-0").readFully(data);
    Assert.assertArrayEquals("read key=" + new String(KEY1), DATA1.getBytes(), data.get(new Slice(KEY1)));

    fs.close();
  }

  @Test
  public void testCodec()
  {
    HDHTTestOperator.BucketStreamCodec codec = new HDHTTestOperator.BucketStreamCodec();
    Slice s = codec.toByteArray(new KeyValPair<byte[], byte[]>(KEY0, DATA0.getBytes()));

    HDHTTestOperator.BucketStreamCodec codec2 = new HDHTTestOperator.BucketStreamCodec();
    codec2.fromByteArray(s);
  }

}
