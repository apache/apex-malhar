/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.algo;

/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import java.util.HashMap;

/**
 * This operator reads input from two streams representing all rows from two separate tables. <br>
 * 
 * <br>
 * Tuples with same value for "key" are merged into one tuple. <br>
 * Even though this module produces continuous tuples, at end of window all data is flushed. Thus the data set is windowed
 * and no history is kept of previous windows<br>
 * <br>
 * <b>Ports</b>
 * <b>data1</b>: expects HashMap<K,V><br>
 * <b>data2</b>: expects HashMap<K,V><br>
 * <b>result</b>: emits HashMap<K,V><br>
 * <b>Properties</b>:<br>
 * <b>key</b>: The key to "joinby"<br>
 * <b>filter1</b>: The keys from port data1 to include<br>
 * <b>filter2</b>: The keys from port data2 to include<br>
 * <br>
 */
public class InnerJoin<K,V> implements  InputOperator
{
  @InputPortFieldAnnotation(name = "data1")
  public final transient DefaultInputPort<HashMap<K,V>> data1 = new DefaultInputPort<HashMap<K,V>>()
  {

		@Override
		public void process(HashMap<K, V> tuple)
		{
			// TODO Auto-generated method stub
			
		}
  };
  @InputPortFieldAnnotation(name = "data2")
  public final transient DefaultInputPort<HashMap<K,V>> data2 = new DefaultInputPort<HashMap<K,V>>()
  {

		@Override
		public void process(HashMap<K, V> tuple)
		{
			// TODO Auto-generated method stub
			
		}
  };
  @OutputPortFieldAnnotation(name = "result")
  public final transient DefaultOutputPort<HashMap<K, V>> result = new DefaultOutputPort<HashMap<K, V>>();
  
	@Override
	public void beginWindow(long windowId)
	{
		// TODO Auto-generated method stub
		
	}
	@Override
	public void endWindow()
	{
		// TODO Auto-generated method stub
		
	}
	@Override
	public void setup(OperatorContext context)
	{
		// TODO Auto-generated method stub
		
	}
	@Override
	public void teardown()
	{
		// TODO Auto-generated method stub
		
	}
	@Override
	public void emitTuples()
	{
		// TODO Auto-generated method stub
		
	}


}
