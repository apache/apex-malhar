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
package com.datatorrent.lib.io.fs;

import org.junit.Assert;
import org.junit.Test;
import com.datatorrent.lib.testbench.RandomWordGenerator;

import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.plan.logical.LogicalPlan;

/**
 * Test class to test {@link AbstractSynchronizer}
 *
 */
public class AbstractSynchronizerTest
{
  public static class TestSynchronizer extends AbstractSynchronizer<byte[], TestMeta>
  {
    private long currentCommittedWindow;
    private StringBuilder windowData = new StringBuilder(100);

    @Override
    protected void processTuple(byte[] input)
    {
      if (windowData.length() > 0) {
        windowData.append(",");
      }
      windowData.append(input);
    }

    @Override
    public void endWindow()
    {
      TestMeta meta = new TestMeta();
      meta.windowid = currentWindowId;
      meta.data = windowData.toString();

      enqueueForProcessing(meta);

      windowData.setLength(0);
    }

    @Override
    protected void processCommittedData(TestMeta meta)
    {
      // only committed windows have to be processed
      Assert.assertTrue("processing window id should not be greater than commited window id", meta.windowid <= currentCommittedWindow);
    }

    @Override
    public void committed(long l)
    {
      currentCommittedWindow = l;
      super.committed(l);
    }

  }

  public static class TestMeta
  {
    long windowid;
    String data;
  }

  @Test
  public void testSynchronizer()
  {
    try {
      LogicalPlan dag = new LogicalPlan();

      RandomWordGenerator generator = dag.addOperator("words", new RandomWordGenerator());
      TestSynchronizer synchronizer = dag.addOperator("synchronizer", new TestSynchronizer());

      generator.setTuplesPerWindow(10);

      dag.addStream("toWriter", generator.output, synchronizer.input);

      StramLocalCluster slc = new StramLocalCluster(dag);

      slc.run(150000); // test assert in synchronizer.processCommittedData to ensure it is called only for TestMeta with committed window ids
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

}