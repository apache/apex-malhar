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
package org.apache.apex.benchmark.window;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.fileaccess.TFileImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableComplexComponentImpl;
import org.apache.apex.malhar.lib.state.spillable.SpillableSetMultimapImpl;
import org.apache.apex.malhar.lib.state.spillable.managed.ManagedStateSpillableStateStore;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.utils.serde.GenericSerde;
import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Tuple.TimestampedTuple;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowedStorage;
import org.apache.apex.malhar.lib.window.accumulation.Count;
import org.apache.apex.malhar.lib.window.impl.InMemoryWindowedKeyedStorage;
import org.apache.apex.malhar.lib.window.impl.KeyedWindowedOperatorImpl;
import org.apache.apex.malhar.lib.window.impl.SpillableWindowedKeyedStorage;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.annotation.ApplicationAnnotation;

/**
 * @since 3.7.0
 */
@ApplicationAnnotation(name = "KeyedWindowedOperatorBenchmark")
public class KeyedWindowedOperatorBenchmarkApp extends AbstractWindowedOperatorBenchmarkApp<
    KeyedWindowedOperatorBenchmarkApp.KeyedWindowedGenerator, KeyedWindowedOperatorBenchmarkApp.MyKeyedWindowedOperator>
{
  public KeyedWindowedOperatorBenchmarkApp()
  {
    generatorClass = KeyedWindowedGenerator.class;
    windowedOperatorClass = KeyedWindowedOperatorBenchmarkApp.MyKeyedWindowedOperator.class;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void connectGeneratorToWindowedOperator(DAG dag, KeyedWindowedGenerator generator,
      KeyedWindowedOperatorBenchmarkApp.MyKeyedWindowedOperator windowedOperator)
  {
    dag.addStream("Data", generator.data, windowedOperator.input).setLocality(Locality.CONTAINER_LOCAL);
  }

  @Override
  protected void setUpdatedKeyStorage(MyKeyedWindowedOperator windowedOperator,
      Configuration conf, SpillableComplexComponentImpl sccImpl)
  {
    windowedOperator.setUpdatedKeyStorage(createUpdatedDataStorage(conf, sccImpl));
  }

  protected static class MyKeyedWindowedOperator extends KeyedWindowedOperatorImpl
  {
    private static final Logger logger = LoggerFactory.getLogger(MyKeyedWindowedOperator.class);

    private long logWindows = 20;
    private long windowCount = 0;
    private long beginTime = System.currentTimeMillis();
    private long tupleCount = 0;
    private long totalBeginTime = System.currentTimeMillis();
    private long totalCount = 0;

    private long droppedCount = 0;
    @Override
    public void dropTuple(Tuple input)
    {
      droppedCount++;
    }

    @Override
    public void endWindow()
    {
      super.endWindow();
      if (++windowCount == logWindows) {
        long endTime = System.currentTimeMillis();
        tupleCount -= droppedCount;
        totalCount += tupleCount;
        logger.info("total: count: {}; time: {}; average: {}; period: count: {}; dropped: {}; time: {}; average: {}",
            totalCount, endTime - totalBeginTime, totalCount * 1000 / (endTime - totalBeginTime),
            tupleCount, droppedCount, endTime - beginTime, tupleCount * 1000 / (endTime - beginTime));
        windowCount = 0;
        beginTime = System.currentTimeMillis();
        tupleCount = 0;
        droppedCount = 0;
      }
    }

    @Override
    public void processTuple(Tuple tuple)
    {
      super.processTuple(tuple);
      ++tupleCount;
    }
  }

  protected static class KeyedWindowedGenerator extends
      AbstractGenerator<Tuple.TimestampedTuple<KeyValPair<String, Long>>>
  {
    @Override
    protected TimestampedTuple<KeyValPair<String, Long>> generateNextTuple()
    {
      return new Tuple.TimestampedTuple<KeyValPair<String, Long>>(System.currentTimeMillis() - random.nextInt(60000),
          new KeyValPair<String, Long>("" + random.nextInt(100000), (long)random.nextInt(100)));
    }
  }

  @Override
  protected Accumulation createAccumulation()
  {
    return new Count();
  }

  private boolean useInMemoryStorage = false;

  @Override
  protected WindowedStorage createDataStorage(SpillableComplexComponentImpl sccImpl)
  {
    if (useInMemoryStorage) {
      return new InMemoryWindowedKeyedStorage();
    }
    SpillableWindowedKeyedStorage dataStorage = new SpillableWindowedKeyedStorage();
    dataStorage.setSpillableComplexComponent(sccImpl);
    return dataStorage;
  }

  protected SpillableSetMultimapImpl<Window, String> createUpdatedDataStorage(Configuration conf,
      SpillableComplexComponentImpl sccImpl)
  {
    String basePath = getStoreBasePath(conf);
    ManagedStateSpillableStateStore store = new ManagedStateSpillableStateStore();
    ((TFileImpl.DTFileImpl)store.getFileAccess()).setBasePath(basePath);

    SpillableSetMultimapImpl<Window, String> dataStorage = new SpillableSetMultimapImpl<Window, String>(store,
        new byte[] {(byte)1}, 0, new GenericSerde<Window>(), new GenericSerde<String>());
    return dataStorage;
  }

  @Override
  protected WindowedStorage createRetractionStorage(SpillableComplexComponentImpl sccImpl)
  {
    if (useInMemoryStorage) {
      return new InMemoryWindowedKeyedStorage();
    }
    SpillableWindowedKeyedStorage retractionStorage = new SpillableWindowedKeyedStorage();
    retractionStorage.setSpillableComplexComponent(sccImpl);
    return retractionStorage;
  }

}
