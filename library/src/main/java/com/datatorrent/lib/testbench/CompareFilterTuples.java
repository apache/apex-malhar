/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.testbench;

import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;

/**
 * <p>Implements Compare Filter Tuples class.</p>
 * <p>
 * @displayName: Compare Filter Tuples
 * @category: testbench
 * @tag: map, compare
 * @since 0.3.2
 */
public class CompareFilterTuples<k> extends BaseOperator
{
	// Compare type function
  private int compareType = 1;
  public void setCompareType(int type)
  {
  	assert(type == 0 || type == 1 || type == -1);
  	compareType = type;
  }
  
  // compare value  
  private int value;
  public void setValue(int value)
  {
  	this.value = value;
  }
  
  // Collected result tuples
  private Map<k, Integer> result;
  
        /**
	 * Input port that takes a map of integer values.
	 */
	public final transient DefaultInputPort<Map<k, Integer>> inport = new DefaultInputPort<Map<k, Integer>>() {
    @Override
    public void process(Map<k, Integer> map) {
    	for(Map.Entry<k, Integer> entry : map.entrySet())
    	{
    		switch(compareType)
    		{
    			case 0 : if(entry.getValue().intValue() == value) result.put(entry.getKey(), entry.getValue()); break;
    			case 1 : if(entry.getValue().intValue() > value) result.put(entry.getKey(), entry.getValue()); break;
    			case -1 : if(entry.getValue().intValue() < value) result.put(entry.getKey(), entry.getValue()); break;
    		}
    	}
    }
	};
	
	/**
	 * Output port that emits a map of integer values.
	 */
	public final transient DefaultOutputPort<Map<k, Integer>> outport = new DefaultOutputPort<Map<k, Integer>>();
        
        /**
	 * Output redis port that emits a map of &lt;integer,string&gt; values.
	 */
	public final transient DefaultOutputPort<Map<Integer, String>> redisport = new DefaultOutputPort<Map<Integer, String>>();
	
	@Override
	public void beginWindow(long windowId)
	{
		result  = new HashMap<k, Integer>();
	}
	
	@Override
	public void endWindow()
	{
		outport.emit(result);
		
		int numOuts = 1;
		Integer total = 0;
		for (Map.Entry<k, Integer>  entry : result.entrySet())
		{
			Map<Integer, String> tuple = new HashMap<Integer, String>();
			tuple.put(numOuts++, entry.getKey().toString());
			redisport.emit(tuple);
			total += entry.getValue();
		}
		Map<Integer, String> tuple = new HashMap<Integer, String>();
		tuple.put(numOuts++, total.toString());
		redisport.emit(tuple);
		tuple = new HashMap<Integer, String>();
		tuple.put(0, new Integer(numOuts).toString());
		redisport.emit(tuple);
	}
}
