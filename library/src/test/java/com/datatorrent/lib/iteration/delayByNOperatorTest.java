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

package com.datatorrent.lib.iteration;

import com.datatorrent.api.*;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.helper.OperatorContextTestHelper;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.testbench.RandomEventGenerator;
import com.datatorrent.lib.util.WindowDataManager;
import com.datatorrent.stram.engine.WindowGenerator;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestWatcher;
import org.junit.runner.Description;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class delayByNOperatorTest {

  public static OperatorContextTestHelper.TestIdOperatorContext context;

  @Rule
  public TestMeta testMeta = new TestMeta();

  public static class TestMeta extends TestWatcher
  {

    @Override
    protected void starting(org.junit.runner.Description description)
    {
      String methodName = description.getMethodName();
      String className = description.getClassName();

      Attribute.AttributeMap.DefaultAttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
      attributes.put(Context.DAGContext.APPLICATION_PATH,
              "target/" + className + "/" + methodName + "/" );

      attributes.put(DAG.APPLICATION_ID, "appId");

      context = new OperatorContextTestHelper.TestIdOperatorContext(0, attributes) ;
    }

    @Override
    protected void finished(Description description)
    {
      FileUtils.deleteQuietly(new File("target/" + description.getClassName() + "/" + description.getMethodName()));
    }
  }

  @Test
  public void testIntegrationWithWindowDataManager() throws Exception {

    DelayByNOperator<Integer> DelayByNOperator = new DelayByNOperator<>(2) ;
    DelayByNOperator.setWindowDataManager(new WindowDataManager.FSWindowDataManager());

    DelayByNOperator.setup(context);

    CollectorTestSink testSink = new CollectorTestSink();
    DelayByNOperator.output.setSink(testSink);

    DelayByNOperator.beginWindow(1);
    DelayByNOperator.input.process(1);
    DelayByNOperator.input.process(2);
    DelayByNOperator.endWindow();

    DelayByNOperator.beginWindow(2);
    DelayByNOperator.input.process(3);
    DelayByNOperator.input.process(4);
    DelayByNOperator.endWindow();

    DelayByNOperator.beginWindow(3);
    DelayByNOperator.endWindow();

    DelayByNOperator.setWindowDataManager(new WindowDataManager.FSWindowDataManager());

    DelayByNOperator.setup(context);

    testSink = new CollectorTestSink();
    DelayByNOperator.output.setSink(testSink);

    DelayByNOperator.firstWindow();

    Assert.assertEquals("required tuples - replayed", 1, testSink.collectedTuples.get(0));
    Assert.assertEquals("required tuples - replayed", 2, testSink.collectedTuples.get(1));

    DelayByNOperator.beginWindow(4);
    DelayByNOperator.endWindow();

    testSink.clear();

    DelayByNOperator.firstWindow();

    Assert.assertEquals("required tuples - replayed", testSink.collectedTuples.get(0), 3);
    Assert.assertEquals("required tuples - replayed", testSink.collectedTuples.get(1), 4);

    DelayByNOperator.beginWindow(WindowGenerator.MAX_WINDOW_ID);
    DelayByNOperator.input.process(1);
    DelayByNOperator.input.process(2);
    DelayByNOperator.endWindow();

    DelayByNOperator.beginWindow(WindowGenerator.MIN_WINDOW_ID);
    DelayByNOperator.input.process(3);
    DelayByNOperator.input.process(4);
    DelayByNOperator.endWindow();

    DelayByNOperator.beginWindow(WindowGenerator.MIN_WINDOW_ID + 1);
    DelayByNOperator.input.process(5);
    DelayByNOperator.input.process(6);
    DelayByNOperator.endWindow();

    DelayByNOperator.setWindowDataManager(new WindowDataManager.FSWindowDataManager());

    DelayByNOperator.setup(context);

    testSink = new CollectorTestSink();
    DelayByNOperator.output.setSink(testSink);

    DelayByNOperator.firstWindow();

    Assert.assertEquals("required tuples - replayed", 1, testSink.collectedTuples.get(0));
    Assert.assertEquals("required tuples - replayed", 2, testSink.collectedTuples.get(1));

    testSink.clear();

    DelayByNOperator.beginWindow(WindowGenerator.MIN_WINDOW_ID + 2);
    DelayByNOperator.input.process(5);
    DelayByNOperator.input.process(6);
    DelayByNOperator.endWindow();

    Assert.assertEquals("required tuples - replayed", 5, testSink.collectedTuples.get(0));
    Assert.assertEquals("required tuples - replayed", 6, testSink.collectedTuples.get(1));
  }

  private void setup ( DelayByNOperator<Integer> DelayByNOperator) {

    DelayByNOperator.setWindowDataManager(new WindowDataManager.FSWindowDataManager());

    DelayByNOperator.setup(context);

    CollectorTestSink testSink = new CollectorTestSink();
    DelayByNOperator.output.setSink(testSink);

    DelayByNOperator.beginWindow(WindowGenerator.MAX_WINDOW_ID - 5 );
    DelayByNOperator.input.process(1);
    DelayByNOperator.input.process(2);
    DelayByNOperator.endWindow();

    DelayByNOperator.beginWindow(WindowGenerator.MAX_WINDOW_ID - 4 );
    DelayByNOperator.input.process(1);
    DelayByNOperator.input.process(2);
    DelayByNOperator.endWindow();

    DelayByNOperator.beginWindow(WindowGenerator.MAX_WINDOW_ID - 3 );
    DelayByNOperator.input.process(1);
    DelayByNOperator.input.process(2);
    DelayByNOperator.endWindow();

    DelayByNOperator.beginWindow(WindowGenerator.MAX_WINDOW_ID - 2 );
    DelayByNOperator.input.process(1);
    DelayByNOperator.input.process(2);
    DelayByNOperator.endWindow();

    DelayByNOperator.beginWindow(WindowGenerator.MAX_WINDOW_ID - 1 );
    DelayByNOperator.input.process(1);
    DelayByNOperator.input.process(2);
    DelayByNOperator.endWindow();

    DelayByNOperator.beginWindow(WindowGenerator.MAX_WINDOW_ID);
    DelayByNOperator.input.process(1);
    DelayByNOperator.input.process(2);
    DelayByNOperator.endWindow();

    DelayByNOperator.beginWindow(WindowGenerator.MIN_WINDOW_ID);
    DelayByNOperator.input.process(1);
    DelayByNOperator.input.process(2);
    DelayByNOperator.endWindow();

    DelayByNOperator.beginWindow(WindowGenerator.MIN_WINDOW_ID + 1);
    DelayByNOperator.input.process(1);
    DelayByNOperator.input.process(2);
    DelayByNOperator.endWindow();

    DelayByNOperator.beginWindow(WindowGenerator.MIN_WINDOW_ID + 2);
    DelayByNOperator.input.process(1);
    DelayByNOperator.input.process(2);
    DelayByNOperator.endWindow();

    DelayByNOperator.beginWindow(WindowGenerator.MIN_WINDOW_ID + 3);
    DelayByNOperator.input.process(1);
    DelayByNOperator.input.process(2);
    DelayByNOperator.endWindow();
  }

  @Test
  public void testWindowDataCleanupAfterTwoCommit() throws IOException {
    DelayByNOperator<Integer> DelayByNOperator = new DelayByNOperator<>(3);

    DelayByNOperator = new DelayByNOperator<>(3);
    setup(DelayByNOperator);
    DelayByNOperator.committed(WindowGenerator.MAX_WINDOW_ID-2);
    DelayByNOperator.committed(2);

    WindowDataManager windowDataManager = DelayByNOperator.getWindowDataManager();
    HashMap<Integer, Object> ret = (HashMap<Integer, Object>)  windowDataManager.load(WindowGenerator.MAX_WINDOW_ID-2);

    Assert.assertEquals("Window Should be cleared", ret.size(), 0);

  }

  @Test
  public void testWindowDataCleanupAfterOneCommit() throws IOException {

    DelayByNOperator<Integer> DelayByNOperator = new DelayByNOperator<>(3);

    setup(DelayByNOperator);

    WindowDataManager windowDataManager = DelayByNOperator.getWindowDataManager();
    Object obj =  windowDataManager.load(WindowGenerator.MAX_WINDOW_ID-4);
    Assert.assertNotEquals("Window Should not be empty", obj, null);

    DelayByNOperator.committed(WindowGenerator.MAX_WINDOW_ID);

    windowDataManager = DelayByNOperator.getWindowDataManager();
    HashMap<Integer, Object> ret = (HashMap<Integer, Object>)  windowDataManager.load(WindowGenerator.MAX_WINDOW_ID-4);

    Assert.assertEquals("Window Should be cleared", ret.size(), 0);
  }

  @Test
  public void testWindowDelay() throws Exception
  {
    for ( int i = 1 ; i < 5 ; ++i ) {
      LocalMode lma = LocalMode.newInstance();
      Application.delay = i;
      Configuration conf = new Configuration(false);
      lma.prepareDAG(new Application(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(10000);
    }
  }

  @ApplicationAnnotation(name="IterationDemo")
  public static class Application implements StreamingApplication
  {
    private final static Logger LOG = LoggerFactory.getLogger(Application.class);
    public static int delay = 1;

    public static class OutputOperator extends BaseOperator
    {
      public long windowId ;

      public transient DefaultInputPort<Long> input = new DefaultInputPort<Long>()
      {
        @Override
        public void process(Long tuple)
        {

          Assert.assertEquals("number emitted tuples", windowId - delay, tuple.longValue());
        }
      };

      public void beginWindow(long windowId)
      {
        this.windowId = windowId;
      }

      public transient DefaultInputPort<Integer> inputDummy = new DefaultInputPort<Integer>()
      {
        @Override
        public void process(Integer tuple)
        {
          output.emit(windowId);
        }
      };
      public transient DefaultOutputPort<Long> output = new DefaultOutputPort<>() ;

    }

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      RandomEventGenerator rand = dag.addOperator("rand", new RandomEventGenerator());
      rand.setTuplesBlast(1);
      rand.setTuplesBlastIntervalMillis(1000);
      DelayByNOperator<Long> DelayByNOperator = dag.addOperator("delay", new DelayByNOperator<Long>(delay));
      OutputOperator outputOperator = dag.addOperator("output", new OutputOperator());

      DelayByNOperator.setWindowDataManager(new WindowDataManager.NoopWindowDataManager());

      dag.addStream("rand_inputToDelay", rand.integer_data, outputOperator.inputDummy);
      dag.addStream("inputToDelay_toDelay", outputOperator.output, DelayByNOperator.input);
      dag.addStream("DelayOperator_output", DelayByNOperator.output, outputOperator.input);
    }
  }
}
