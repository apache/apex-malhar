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
import java.nio.ByteBuffer;

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
import com.datatorrent.contrib.hds.HDSTest.MyDataKey;
import com.datatorrent.lib.util.KeyValPair;

@ApplicationAnnotation(name="HDSTestApp")
public class HDSTestApp implements StreamingApplication
{

  public static class Generator extends BaseOperator implements InputOperator
  {
    public transient DefaultOutputPort<KeyValPair<byte[], byte[]>> output = new DefaultOutputPort<KeyValPair<byte[], byte[]>>();

    @Override
    public void emitTuples()
    {
      byte[] k0 = ByteBuffer.allocate(16).putLong(0x00000000).putLong(0).array();
      output.emit(new KeyValPair<byte[], byte[]>(k0, "data0".getBytes()));
      byte[] k1 = ByteBuffer.allocate(16).putLong(0xffffffff).putLong(0).array();
      output.emit(new KeyValPair<byte[], byte[]>(k1, "data1".getBytes()));
    }
  }

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    Generator generator = dag.addOperator("Generator", new Generator());
    HDSOperator store = dag.addOperator("Store", new HDSOperator());
    store.setKeyComparator(new MyDataKey.SequenceComparator());
    store.setFileStore(new HDSFileAccessFSImpl());
    dag.addStream("Generator2Store", generator.output, store.data);
  }

  @Test
  public void test() throws Exception
  {
    File file = new File("target/hds2");
    FileUtils.deleteDirectory(file);

    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.set("dt.operator.Store.fileStore.basePath", file.toURI().toString());
    conf.set("dt.operator.Store.attr.INITIAL_PARTITION_COUNT", "2");

    lma.prepareDAG(new HDSTestApp(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);
    lc.runAsync();

    long tms = System.currentTimeMillis();
    File f0 = new File(file, "0/_WAL");
    File f1 = new File(file, "1/_WAL");

    while (System.currentTimeMillis() - tms < 30000) {
      if (f0.exists()) break;
      if (f1.exists()) break;
      Thread.sleep(100);
    }
    lc.shutdown();

    Assert.assertTrue("exists " + f0, f0.exists() && f0.isFile());
    Assert.assertTrue("exists " + f1, f1.exists() && f1.isFile());

  }


}
