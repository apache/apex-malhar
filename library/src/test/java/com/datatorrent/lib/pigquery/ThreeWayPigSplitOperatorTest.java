/**
 * Copyright (C) 2015 DataTorrent, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.pigquery;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 * Functional test for {@link com.datatorrent.lib.pigquery.ThreeWayPigSplitOperator}.
 */
@Deprecated
public class ThreeWayPigSplitOperatorTest
{
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
  public void testSqlSelect()
  {
  	// create operator   
	  ThreeWayPigSplit oper = new ThreeWayPigSplit();
  	
  	CollectorTestSink sink = new CollectorTestSink();
  	oper.outports.get(0).setSink(sink);
  	CollectorTestSink sink1 = new CollectorTestSink();
    oper.outports.get(1).setSink(sink1);
    CollectorTestSink sink2 = new CollectorTestSink();
    oper.outports.get(2).setSink(sink2);
  	
  	oper.setup(null);
  	oper.beginWindow(1);
  	
  	Map<String, Integer> tuple = new HashMap<String, Integer>();
  	tuple.put("f1", 1);
  	tuple.put("f2", 2);
  	tuple.put("f3", 3);
  	oper.inport.process(tuple);
  	
  	tuple = new HashMap<String, Integer>();
  	tuple.put("f1", 4);
    tuple.put("f2", 5);
    tuple.put("f3", 6);
  	oper.inport.process(tuple);
  	
  	tuple = new HashMap<String, Integer>();
  	tuple.put("f1", 7);
    tuple.put("f2", 8);
    tuple.put("f3", 2);
  	oper.inport.process(tuple);
  	
  	oper.endWindow();
  	oper.teardown();
  	
  	System.out.println(sink.collectedTuples.toString());
  	System.out.println(sink1.collectedTuples.toString());
  	System.out.println(sink2.collectedTuples.toString());
  }
}
