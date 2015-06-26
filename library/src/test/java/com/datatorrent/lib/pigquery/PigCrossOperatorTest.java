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

import org.junit.Test;

import com.datatorrent.lib.testbench.CollectorTestSink;

/**
 * Functional test for {@link com.datatorrent.lib.streamquery.PigGroupOperator}.
 */
@Deprecated
public class PigCrossOperatorTest
{
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
  public void testSqlSelect()
  {
  	// create operator   
	  PigCrossOperator oper = new PigCrossOperator();
  	
  	CollectorTestSink sink = new CollectorTestSink();
  	oper.outport.setSink(sink);
  	
  	oper.setup(null);
  	oper.beginWindow(1);
  	
  	HashMap<String, Object> tuple = new HashMap<String, Object>();
  	tuple.put("a", 0);
  	tuple.put("b", 1);
  	tuple.put("c", 2);
  	oper.inport1.process(tuple);
  	
  	tuple = new HashMap<String, Object>();
  	tuple.put("a", 0);
  	tuple.put("b", 3);
  	tuple.put("c", 4);
  	oper.inport1.process(tuple);
  	
  	tuple = new HashMap<String, Object>();
  	tuple.put("e", 1);
  	tuple.put("f", 5);
  	tuple.put("g", 6);
  	oper.inport2.process(tuple);
  	
  	tuple = new HashMap<String, Object>();
    tuple.put("e", 1);
    tuple.put("f", 7);
    tuple.put("g", 8);
    oper.inport2.process(tuple);
    
  	oper.endWindow();
  	oper.teardown();
  	
  	System.out.println(sink.collectedTuples.toString());
  }
}
