/*
 *  Copyright (c) 2012 Malhar, Inc. All Rights Reserved.
 */
package com.malhartech.lib.testbench;

import java.util.HashMap;
import java.util.Map;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.DefaultOutputPort;

/**
 * @author Dinesh Prasad (dinesh@malhar-inc.com)
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
  
  // input port
	public final transient DefaultInputPort<Map<k, Integer>> inport = new DefaultInputPort<Map<k, Integer>>(this) {
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
	
	//out port
	public final transient DefaultOutputPort<Map<k, Integer>> outport = new DefaultOutputPort<Map<k, Integer>>(this);
	public final transient DefaultOutputPort<Map<Integer, String>> redisport = new DefaultOutputPort<Map<Integer, String>>(this);
	
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
