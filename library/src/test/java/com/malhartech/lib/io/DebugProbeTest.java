/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 */
package com.malhartech.lib.io;

import com.malhartech.dag.OperatorConfiguration;
import java.util.ArrayList;
import java.util.HashMap;
import junit.framework.Assert;
import org.junit.Test;


public class DebugProbeTest  {

  @Test
  public void testDebugProbeNode() throws Exception
  {

    DebugProbe node = new DebugProbe();

    OperatorConfiguration conf = new OperatorConfiguration("testNode", new HashMap<String, String>());
    conf.setInt("SpinMillis", 10);
    conf.setInt("BufferCapacity", 1024 * 1024);

    node.setup(conf);

    HashMap<String, Double> hinput = new HashMap<String, Double>();
    String sinput = "";
    Double dinput = new Double(0.0);
    Integer iinput = new Integer(1);
    ArrayList<?> ainput = new ArrayList<Object>();

    int sentval = 0;
    for (int i = 0; i < 100; i++) {sentval += 1; node.process(hinput);}
    for (int i = 0; i < 200; i++) {sentval += 1; node.process(sinput);}
    for (int i = 0; i < 300; i++) {sentval += 1; node.process(iinput);}
    for (int i = 0; i < 400; i++) {sentval += 1; node.process(dinput);}
    for (int i = 0; i < 500; i++) {sentval += 1; node.process(ainput);}
    node.endWindow();
    Assert.assertEquals("number emitted tuples", sentval, node.getCount());

    node.teardown();
  }
}
