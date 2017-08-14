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

import org.apache.apex.malhar.lib.state.spillable.SpillableComplexComponentImpl;
import org.apache.apex.malhar.lib.window.Accumulation;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Tuple.TimestampedTuple;
import org.apache.apex.malhar.lib.window.WindowedStorage;
import org.apache.apex.malhar.lib.window.accumulation.Count;
import org.apache.apex.malhar.lib.window.impl.SpillableWindowedPlainStorage;
import org.apache.apex.malhar.lib.window.impl.WindowedOperatorImpl;

import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.annotation.ApplicationAnnotation;

@ApplicationAnnotation(name = "WindowedOperatorBenchmark")
/**
 * @since 3.7.0
 */
public class WindowedOperatorBenchmarkApp extends AbstractWindowedOperatorBenchmarkApp<
    WindowedOperatorBenchmarkApp.WindowedGenerator, WindowedOperatorBenchmarkApp.MyWindowedOperator>
{
  public WindowedOperatorBenchmarkApp()
  {
    generatorClass = WindowedGenerator.class;
    windowedOperatorClass = WindowedOperatorBenchmarkApp.MyWindowedOperator.class;
  }


  protected static class WindowedGenerator extends AbstractGenerator<Tuple.TimestampedTuple<Long>>
  {
    @Override
    protected TimestampedTuple<Long> generateNextTuple()
    {
      return new Tuple.TimestampedTuple<Long>(System.currentTimeMillis() - random.nextInt(120000),
          (long)random.nextInt(100));
    }
  }

  protected static class MyWindowedOperator extends WindowedOperatorImpl
  {
    private static final Logger logger = LoggerFactory.getLogger(MyWindowedOperator.class);

    private long logWindows = 20;
    private long windowCount = 0;
    private long beginTime = System.currentTimeMillis();
    private long tupleCount = 0;
    private long totalBeginTime = System.currentTimeMillis();
    private long totalCount = 0;

    @Override
    public void endWindow()
    {
      super.endWindow();
      if (++windowCount == logWindows) {
        long endTime = System.currentTimeMillis();
        totalCount += tupleCount;
        logger.info("total: count: {}; time: {}; average: {}; period: count: {}; time: {}; average: {}",
            totalCount, endTime - totalBeginTime, totalCount * 1000 / (endTime - totalBeginTime),
            tupleCount, endTime - beginTime, tupleCount * 1000 / (endTime - beginTime));
        windowCount = 0;
        beginTime = System.currentTimeMillis();
        tupleCount = 0;
      }
    }

    @Override
    public void processTuple(Tuple tuple)
    {
      super.processTuple(tuple);
      ++tupleCount;
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  protected void connectGeneratorToWindowedOperator(DAG dag, WindowedGenerator generator,
      MyWindowedOperator windowedOperator)
  {
    dag.addStream("Data", generator.data, windowedOperator.input).setLocality(Locality.CONTAINER_LOCAL);
  }


  @Override
  protected Accumulation createAccumulation()
  {
    return new Count();
  }


  @Override
  protected WindowedStorage createDataStorage(SpillableComplexComponentImpl sccImpl)
  {
    SpillableWindowedPlainStorage plainDataStorage = new SpillableWindowedPlainStorage();
    plainDataStorage.setSpillableComplexComponent(sccImpl);
    return plainDataStorage;
  }


  @Override
  protected WindowedStorage createRetractionStorage(SpillableComplexComponentImpl sccImpl)
  {
    SpillableWindowedPlainStorage plainRetractionStorage = new SpillableWindowedPlainStorage();
    plainRetractionStorage.setSpillableComplexComponent(sccImpl);
    return plainRetractionStorage;
  }
}
