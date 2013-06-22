/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.lib.algo;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.algo.InnerJoin2;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.stram.StramLocalCluster;
import com.datatorrent.stram.plan.logical.LogicalPlan;
import com.datatorrent.stram.support.StramTestSupport;
import com.datatorrent.stram.support.StramTestSupport.WaitCondition;
import java.util.ArrayList;
import java.util.HashMap;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class InnerJoin2Test
{
  public static class SourceModule1 extends BaseOperator
  implements InputOperator
  {

    public final transient DefaultOutputPort<HashMap<String, String>> outPort = new DefaultOutputPort<HashMap<String, String>>();
    int testNum=6;
    int start;

    @Override
    public void setup(OperatorContext context)
    {
    }

    @Override
    public void beginWindow(long windowId) {
      System.out.println("Source1 beginWindow:"+windowId);
    }
    @Override
    public void endWindow() {
      System.out.println("Source1 endWindow");
    }

    public void emitTuple(HashMap<String, String> message)
    {
      outPort.emit(message);
//      System.out.println("emitting "+message.toString());
    }

    @Override
    public void emitTuples()
    {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put(Integer.toString(31), "Rafferty");
        emitTuple(map);
        map.clear();
        map.put(Integer.toString(33), "Jones");
        emitTuple(map);
        map.clear();
        map.put(Integer.toString(33), "Steinberg");
        emitTuple(map);
        map.clear();
        map.put(Integer.toString(34), "Robinson");
        emitTuple(map);
        map.clear();
        map.put(Integer.toString(34), "Smith");
        emitTuple(map);
        map.clear();
        map.put(Integer.toString(-1), "John");
        emitTuple(map);
//      for( int i=start; i<start+testNum; i++ ) {
//        HashMap<String, String> map = new HashMap<String, String>();
//        map.put(Integer.toString(i), "1 number"+i);
//        emitTuple(map);
//      }
//        System.out.println("emitting Souce1");
    }

    public void setTestNum(int testNum)
    {
      this.testNum = testNum;
    }

    public void setStart(int start) {
      this.start = start;
    }
  }

  public static class SourceModule2 extends BaseOperator
  implements InputOperator
  {
    public final transient DefaultOutputPort<HashMap<String, String>> outPort = new DefaultOutputPort<HashMap<String, String>>();
    int testNum=5;
    int start;

    @Override
    public void setup(OperatorContext context)
    {
    }
    @Override
    public void beginWindow(long windowId) {
      System.out.println("Source2 beginWindow:"+windowId);
    }
    @Override
    public void endWindow() {
      System.out.println("Source2 endWindow");
    }

    public void emitTuple(HashMap<String, String> message)
    {
      outPort.emit(message);
    }

    @Override
    public void emitTuples()
    {
        HashMap<String, String> map = new HashMap<String, String>();
        map.put(Integer.toString(31), "Sales");
        emitTuple(map);
        map.clear();
        map.put(Integer.toString(33), "Engineering");
        emitTuple(map);
        map.clear();
        map.put(Integer.toString(34), "Clerical");
        emitTuple(map);
        map.clear();
        map.put(Integer.toString(35), "Marketing");
        emitTuple(map);
        map.clear();
//      for( int i=start; i<start+testNum; i++ ) {
//        HashMap<String, String> map = new HashMap<String, String>();
//        map.put(Integer.toString(i), "2 number"+i);
//        emitTuple(map);
//      }
//        System.out.println("emitting Souce2");
    }

    public void setTestNum(int testNum)
    {
      this.testNum = testNum;
    }

    public void setStart(int start) {
      this.start = start;
    }
  }

  @Test
  public void testDag() throws Exception
  {
    LogicalPlan dag = new LogicalPlan();
    SourceModule1 input1 = dag.addOperator("input1", SourceModule1.class);
    SourceModule2 input2 = dag.addOperator("input2", SourceModule2.class);
//    input1.setStart(0);
//    input2.setStart(3);
    final InnerJoin2<String, String> join = dag.addOperator("join", new InnerJoin2<String, String>());

    ConsoleOutputOperator output = dag.addOperator("console", ConsoleOutputOperator.class);

    dag.addStream("stream1", input1.outPort, join.data1);
    dag.addStream("stream2", input2.outPort, join.data2);
    dag.addStream("output", join.result, output.input);

    final StramLocalCluster localCluster = new StramLocalCluster(dag);
    localCluster.runAsync();
    WaitCondition c = new WaitCondition()
    {
      @Override
      public boolean isComplete()
      {
        return false;
      }
    };
     StramTestSupport.awaitCompletion(c, 3000);
//    Assert.assertTrue("Tuple recorder shouldn't exist any more after stopping", StramTestSupport.awaitCompletion(c, 4000));

  }
}