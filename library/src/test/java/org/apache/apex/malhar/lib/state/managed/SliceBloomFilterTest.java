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

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

import org.apache.apex.malhar.lib.utils.serde.SerializationBuffer;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.netlet.util.Slice;

public class SliceBloomFilterTest
{
  private int loop = 100000;

  @Test
  public void testBloomFilterForBytes()
  {
    final int maxSliceLength = 1000;
    Random random = new Random();
    final byte[] bytes = new byte[loop + maxSliceLength];
    random.nextBytes(bytes);

    long beginTime = System.currentTimeMillis();
    SliceBloomFilter bloomFilter = new SliceBloomFilter(100000, 0.99);
    for (int i = 0; i < loop; i++) {
      bloomFilter.put(new Slice(bytes, i, i % maxSliceLength + 1));
    }

    for (int i = 0; i < loop; i++) {
      Assert.assertTrue(bloomFilter.mightContain(new Slice(bytes, i, i % maxSliceLength + 1)));
    }
  }

  @Test
  public void testBloomFilterForInt()
  {
    testBloomFilterForInt(2);
    testBloomFilterForInt(3);
    testBloomFilterForInt(5);
    testBloomFilterForInt(7);
  }

  public void testBloomFilterForInt(int span)
  {
    double expectedFalseProbability = 0.3;
    SerializationBuffer buffer = SerializationBuffer.READ_BUFFER;

    SliceBloomFilter bloomFilter = new SliceBloomFilter(loop, expectedFalseProbability);

    for (int i = 0; i < loop; i++) {
      if (i % span == 0) {
        buffer.writeInt(i);
        bloomFilter.put(buffer.toSlice());
      }
    }
    buffer.getWindowedBlockStream().releaseAllFreeMemory();

    int falsePositive = 0;
    for (int i = 0; i < loop; i++) {
      buffer.writeInt(i);
      if (!bloomFilter.mightContain(buffer.toSlice())) {
        Assert.assertTrue(i % span != 0);
      } else {
        // BF says its present
        if (i % 2 != 0) {
          // But was not there
          falsePositive++;
        }
      }
    }
    buffer.getWindowedBlockStream().releaseAllFreeMemory();
    // Verify false positive prob
    double falsePositiveProb = falsePositive;
    falsePositiveProb /= loop;
    Assert.assertTrue(falsePositiveProb <= expectedFalseProbability);

    for (int i = 0; i < loop; i++) {
      if (i % span == 0) {
        buffer.writeInt(i);
        Assert.assertTrue(bloomFilter.mightContain(buffer.toSlice()));
      }
    }
    buffer.getWindowedBlockStream().releaseAllFreeMemory();
  }

  private static class FilterOperator extends BaseOperator
  {
    private SliceBloomFilter bloomFilter = new SliceBloomFilter(10000, 0.99);
    private SerializationBuffer buffer = SerializationBuffer.READ_BUFFER;

    public final transient DefaultInputPort<String> input = new DefaultInputPort<String>()
    {
      @Override
      public void process(String tuple)
      {
        processTuple(tuple);
      }
    };

    @Override
    public void setup(Context.OperatorContext context)
    {
    }

    private int count = 0;

    public void processTuple(String tuple)
    {
      buffer.writeString(tuple);
      bloomFilter.mightContain(buffer.toSlice());
      buffer.reset();
    }
  }

  private static class TestInputOperator extends BaseOperator implements InputOperator
  {
    public final transient DefaultOutputPort<String> data = new DefaultOutputPort<String>();
    private int current = 0;

    @Override
    public void emitTuples()
    {
      data.emit("" + current++);
    }
  }

  /**
   * Just test SliceBloomFilter can be used by operator. such as it is serializable etc
   * @throws Exception
   */
  @Test
  public void testBloomFilterForApplication() throws Exception
  {
    Configuration conf = new Configuration(false);

    LocalMode lma = LocalMode.newInstance();
    DAG dag = lma.getDAG();

    TestInputOperator generator = new TestInputOperator();
    dag.addOperator("Generator", generator);

    FilterOperator filterOperator = new FilterOperator();
    dag.addOperator("filterOperator", filterOperator);
    dag.addStream("Data", generator.data, filterOperator.input).setLocality(Locality.CONTAINER_LOCAL);

    StreamingApplication app = new StreamingApplication()
    {
      @Override
      public void populateDAG(DAG dag, Configuration conf)
      {
      }
    };

    lma.prepareDAG(app, conf);

    // Create local cluster
    final LocalMode.Controller lc = lma.getController();
    lc.run(3000);

    lc.shutdown();
  }
}
