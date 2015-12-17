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

public class DefaultDelayOperatorTest {

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
      context.setWindowsFromCheckpoint(1);
    }

    @Override
    protected void finished(Description description)
    {
      FileUtils.deleteQuietly(new File("target/" + description.getClassName() + "/" + description.getMethodName()));
    }
  }

  @Test
  public void testIntegrationWithWindowDataManager() throws Exception {

    DefaultDelayOperator<Integer> defaultDelayOperator = new DefaultDelayOperator<>() ;
    defaultDelayOperator.setWindowDataManager(new WindowDataManager.FSWindowDataManager());

    defaultDelayOperator.setup(context);

    CollectorTestSink testSink = new CollectorTestSink();
    defaultDelayOperator.output.setSink(testSink);

    defaultDelayOperator.beginWindow(1);
    defaultDelayOperator.input.process(1);
    defaultDelayOperator.input.process(2);
    defaultDelayOperator.endWindow();

    defaultDelayOperator.beginWindow(2);
    defaultDelayOperator.input.process(3);
    defaultDelayOperator.input.process(4);
    defaultDelayOperator.endWindow();


    defaultDelayOperator = new DefaultDelayOperator<>() ;
    defaultDelayOperator.setWindowDataManager(new WindowDataManager.FSWindowDataManager());

    defaultDelayOperator.setup(context);

    testSink = new CollectorTestSink();
    defaultDelayOperator.output.setSink(testSink);

    defaultDelayOperator.firstWindow(1);

    Assert.assertEquals("required tuples - replayed", testSink.collectedTuples.get(0), 1);
    Assert.assertEquals("required tuples - replayed", testSink.collectedTuples.get(1), 2);

    testSink.collectedTuples.clear();

    defaultDelayOperator.firstWindow(2);

    Assert.assertEquals("required tuples - replayed", testSink.collectedTuples.get(0), 3);
    Assert.assertEquals("required tuples - replayed", testSink.collectedTuples.get(1), 4);

    defaultDelayOperator.teardown();
    defaultDelayOperator.getWindowDataManager().deleteUpTo(context.getId(),2);
  }

  @Test
  public void testOneWindowDelay() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf =new Configuration(false);
    lma.prepareDAG(new Application(), conf);
    LocalMode.Controller lc = lma.getController();

    lc.run(10000);
  }

  @ApplicationAnnotation(name="IterationDemo")
  public static class Application implements StreamingApplication
  {
    private final static Logger LOG = LoggerFactory.getLogger(Application.class);

    public static class OutputOperator extends BaseOperator
    {
      public long windowId ;

      public transient DefaultInputPort<Long> input = new DefaultInputPort<Long>()
      {
        @Override
        public void process(Long tuple)
        {
          Assert.assertEquals("number emitted tuples", windowId-1, tuple.longValue());
        }
      };

      public void beginWindow(long windowId)
      {
        this.windowId = windowId;
      }

    }

    public static class inputToDelay extends BaseOperator
    {
      public long windowId ;

      public transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>()
      {
        @Override
        public void process(Integer tuple)
        {
             output.emit(windowId);
        }
      };
      public transient DefaultOutputPort<Long> output = new DefaultOutputPort<>() ;

      public void beginWindow(long windowId)
      {
        this.windowId = windowId;
      }
    }

    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      RandomEventGenerator rand = dag.addOperator("rand", new RandomEventGenerator());
      rand.setTuplesBlast(1);
      rand.setTuplesBlastIntervalMillis(1000);
      DefaultDelayOperator<Long> defaultDelayOperator = dag.addOperator("delay", new DefaultDelayOperator<Long>());
      OutputOperator outputOperator = dag.addOperator("output", new OutputOperator());
      inputToDelay inputToDelay = dag.addOperator("inputToDelay", new inputToDelay()) ;

      defaultDelayOperator.setWindowDataManager(new WindowDataManager.NoopWindowDataManager());

      dag.addStream("rand_inputToDelay", rand.integer_data, inputToDelay.input);
      dag.addStream("inputToDelay_toDelay", inputToDelay.output, defaultDelayOperator.input);
      dag.addStream("DelayOperator_output", defaultDelayOperator.output, outputOperator.input);
    }
  }
}
