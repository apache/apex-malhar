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
package com.datatorrent.demos.distributeddistinct;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.LocalMode;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.lib.algo.UniqueValueCount;
import com.datatorrent.lib.util.KeyValPair;

public class HDFSUniqueValueCountAppenderTest
{
  static class KeyGen implements InputOperator
  {

    public transient DefaultOutputPort<KeyValPair<Integer, Object>> output = new DefaultOutputPort<KeyValPair<Integer, Object>>();

    @Override
    public void beginWindow(long windowId)
    {
    }

    public void emitKeyVals(int key, int start, int end, int increment)
    {
      for (int i = start; i <= end; i += increment) {
        output.emit(new KeyValPair<Integer, Object>(key, i));
      }
    }

    @Override
    public void endWindow()
    {
    }

    @Override
    public void setup(OperatorContext context)
    {

    }

    @Override
    public void teardown()
    {

    }

    @Override
    public void emitTuples()
    {
      emitKeyVals(1, 1, 10, 1);
      emitKeyVals(2, 3, 15, 3);
      emitKeyVals(3, 2, 20, 2);
      emitKeyVals(1, 5, 15, 1);
      emitKeyVals(2, 11, 20, 1);
      emitKeyVals(3, 11, 20, 1);
    }
  }

  static class VerifyOutput extends BaseOperator
  {
    Set<KeyValPair<Object, Object>> ansSet = new HashSet<KeyValPair<Object, Object>>();
    public final transient DefaultInputPort<KeyValPair<Object, Object>> input = new DefaultInputPort<KeyValPair<Object, Object>>() {

      @Override
      public void process(KeyValPair<Object, Object> tuple)
      {
        ansSet.add(tuple);
      }
    };

    @Override
    public void beginWindow(long windowId)
    {
    }

    @Override
    public void endWindow()
    {
      for (KeyValPair<Object, Object> k : ansSet) {
        if (k.getKey().equals((Object) 1)) {
          Assert.assertEquals(15, k.getValue());
          System.out.println("Should be 15, is " + k.getValue());
        }
        if (k.getKey().equals((Object) 2)) {
          Assert.assertEquals(13, k.getValue());
          System.out.println("Should be 13, is " + k.getValue());
        }
        if (k.getKey().equals((Object) 3)) {
          Assert.assertEquals(15, k.getValue());
          System.out.println("Should be 15, is " + k.getValue());
        }
      }
    }

    @Override
    public void setup(OperatorContext context)
    {
    }

    @Override
    public void teardown()
    {

    }

    public static ArrayList<Integer> processResult(ResultSet resultSet)
    {
      ArrayList<Integer> tempList = new ArrayList<Integer>();
      try {
        while (resultSet.next()) {
          tempList.add(resultSet.getInt(1));
        }
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
      Collections.sort(tempList);
      return tempList;
    }
  }

  public class Application implements StreamingApplication
  {
    @SuppressWarnings("unchecked")
    @Override
    public void populateDAG(DAG dag, Configuration conf)
    {
      KeyGen keyGen = dag.addOperator("KeyGenerator", new KeyGen());
      UniqueValueCount<Integer> valCount = dag.addOperator("ValueCounter", new UniqueValueCount<Integer>());
      HDFSUniqueValueCountAppender<Integer> uniqueUnifier = dag.addOperator("Unique", new HDFSUniqueValueCountAppender<Integer>());
      VerifyOutput verifyOutput = dag.addOperator("VerifyOutput", new VerifyOutput());

      @SuppressWarnings("rawtypes")
      DefaultOutputPort valOut = valCount.output;
      @SuppressWarnings("rawtypes")
      DefaultOutputPort uniqueOut = uniqueUnifier.output;
      dag.addStream("DataIn", keyGen.output, valCount.input);
      dag.addStream("UnifyWindows", valOut, uniqueUnifier.input);
      dag.addStream("ResultsOut", uniqueOut, verifyOutput.input);
    }
  }

  @Test
  public void testApplication() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);

    lma.prepareDAG(new Application(), conf);
    lma.cloneDAG();
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);
    lc.runAsync();

    long now = System.currentTimeMillis();
    while (System.currentTimeMillis() - now < 9000) {
      Thread.sleep(1000);
    }
    lc.shutdown();
  }
}
